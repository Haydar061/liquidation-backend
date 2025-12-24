import asyncio
import json
import time
from collections import defaultdict
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import websockets

BINANCE_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"


class LiquidationStore:
    """
    In-memory store for aggregated liquidation data.

    Structure:
        data[symbol][bucket_ts] = {"long": float, "short": float}
    where bucket_ts is the start of the hour in ms.
    """

    def __init__(self, max_hours: int = 24 * 7) -> None:
        self.data: Dict[str, Dict[int, Dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: {"long": 0.0, "short": 0.0})
        )
        self.max_hours = max_hours
        self.lock = asyncio.Lock()

    async def add(
        self, symbol: str, ts_ms: int, side: str, notional: float
    ) -> None:
        """
        side: "LONG" or "SHORT"
        """
        bucket_ms = (ts_ms // 3_600_000) * 3_600_000  # round to hour
        async with self.lock:
            bucket = self.data[symbol][bucket_ms]
            if side == "LONG":
                bucket["long"] += notional
            else:
                bucket["short"] += notional
            await self._cleanup_locked()

    async def _cleanup_locked(self) -> None:
        """Remove buckets older than max_hours."""
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self.max_hours * 3_600_000
        to_delete_symbols: List[str] = []
        for symbol, buckets in self.data.items():
            old_keys = [ts for ts in buckets.keys() if ts < cutoff]
            for ts in old_keys:
                del buckets[ts]
            if not buckets:
                to_delete_symbols.append(symbol)
        for sym in to_delete_symbols:
            del self.data[sym]

    async def history(
        self, symbol: str, hours: int
    ) -> List[Dict[str, float]]:
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - hours * 3_600_000
        async with self.lock:
            buckets = self.data.get(symbol, {})
            points = []
            for ts, vals in sorted(buckets.items()):
                if ts >= cutoff:
                    points.append(
                        {
                            "ts": ts,
                            "long": vals["long"],
                            "short": vals["short"],
                            "total": vals["long"] + vals["short"],
                        }
                    )
        return points


liq_store = LiquidationStore()

app = FastAPI(title="Haydar Liquidation Engine")


@app.get("/")
async def home():
    return {"status": "Backend Çalışıyor", "message": "Haydar Mode 🔥"}


@app.get("/liquidation/history")
async def get_liquidation_history(
    symbol: str = Query(..., description="Örn: BTCUSDT, ETHUSDT, SOLUSDT"),
    hours: int = Query(
        24, ge=1, le=24 * 7, description="Kaç saatlik geçmiş (1-168)"
    ),
):
    symbol = symbol.upper()
    points = await liq_store.history(symbol, hours)
    return {
        "symbol": symbol,
        "hours": hours,
        "points": points,
        "point_count": len(points),
    }


@app.get("/liquidation/summary")
async def get_liquidation_summary(
    symbol: str = Query(..., description="Örn: BTCUSDT, ETHUSDT, SOLUSDT"),
    hours: int = Query(
        24, ge=1, le=24 * 7, description="Kaç saatlik geçmiş (1-168)"
    ),
):
    symbol = symbol.upper()
    points = await liq_store.history(symbol, hours)
    long_total = sum(p["long"] for p in points)
    short_total = sum(p["short"] for p in points)
    return {
        "symbol": symbol,
        "hours": hours,
        "long_liq": long_total,
        "short_liq": short_total,
        "total_liq": long_total + short_total,
        "point_count": len(points),
    }


async def _handle_binance_event(event: Dict) -> None:
    """
    Parse a single liquidation event from Binance !forceOrder@arr stream
    and push into the store.
    """
    try:
        inner = event.get("o", {})
        symbol = inner.get("s")
        side = inner.get("S")  # BUY / SELL
        avg_price = float(inner.get("ap", "0") or "0")
        qty_last = float(inner.get("z", "0") or "0")
        if qty_last == 0:
            qty_last = float(inner.get("l", "0") or "0")
        ts_ms = int(inner.get("T") or event.get("E") or int(time.time() * 1000))
        if not symbol or avg_price <= 0 or qty_last <= 0 or side not in ("BUY", "SELL"):
            return

        notional = avg_price * qty_last

        # Binance tarafında:
        # - SELL = Long pozisyonun tasfiyesi (LONG likidasyon)
        # - BUY  = Short pozisyonun tasfiyesi (SHORT likidasyon)
        if side == "SELL":
            logical_side = "LONG"
        else:
            logical_side = "SHORT"

        await liq_store.add(symbol, ts_ms, logical_side, notional)
    except Exception:
        # Sessiz yutuyoruz ki stream durmasın
        return


async def binance_liquidation_worker() -> None:
    """
    Connects to Binance all-market liquidation stream and continuously
    aggregates data into the in-memory store.
    """
    while True:
        try:
            print("[Binance] Connecting to liquidation stream...")
            async with websockets.connect(
                BINANCE_WS_URL, ping_interval=20, ping_timeout=20
            ) as ws:
                print("[Binance] Connected.")
                async for raw in ws:
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # !forceOrder@arr bazen tek obje, bazen array dönebilir
                    if isinstance(payload, list):
                        events = payload
                    else:
                        events = [payload]

                    for ev in events:
                        if isinstance(ev, dict):
                            await _handle_binance_event(ev)
        except Exception as e:
            # Bağlantı koptuğunda biraz bekleyip tekrar dene
            print(f"[Binance] Hata: {e}. 5 sn sonra tekrar denenecek.")
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup_event():
    # Binance liquidation stream'ini arka planda başlat
    asyncio.create_task(binance_liquidation_worker())
    print("Startup tamam: Binance liquidation worker başlatıldı.")
