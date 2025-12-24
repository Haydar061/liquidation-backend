import os
import asyncio
import json
import time
from collections import defaultdict
from typing import Dict, List

import httpx
import websockets
from fastapi import FastAPI, Query

# ============================
#  TELEGRAM AYARLARI (ENV)
# ============================

TELEGRAM_BOT_TOKEN = os.getenv("8592409575:AAGvc2hL4FAGrB2VWwPsdwTo371biXyCJnQ")
TELEGRAM_CHAT_ID = os.getenv("7639869416")

# Tekil büyük likidasyon alarmı (USD)
ALERT_MIN_EVENT_USD = float(os.getenv("ALERT_MIN_EVENT_USD", "100000"))
# Saatlik toplam likidasyon alarmı (USD)
ALERT_MIN_HOURLY_USD = float(os.getenv("ALERT_MIN_HOURLY_USD", "2000000"))

BINANCE_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"


class LiquidationStore:
    """
    data[symbol][bucket_ts] = {"long": float, "short": float}
    bucket_ts = hour start in ms
    """

    def __init__(self, max_hours: int = 24 * 7) -> None:
        self.data: Dict[str, Dict[int, Dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: {"long": 0.0, "short": 0.0})
        )
        self.max_hours = max_hours
        self.lock = asyncio.Lock()

    async def add(self, symbol: str, ts_ms: int, side: str, notional: float) -> None:
        bucket_ms = (ts_ms // 3_600_000) * 3_600_000  # 1 saatlik bucket
        async with self.lock:
            bucket = self.data[symbol][bucket_ms]
            if side == "LONG":
                bucket["long"] += notional
            else:
                bucket["short"] += notional
            await self._cleanup_locked()

    async def _cleanup_locked(self) -> None:
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - self.max_hours * 3_600_000
        to_delete: List[str] = []
        for sym, buckets in self.data.items():
            old_keys = [ts for ts in buckets.keys() if ts < cutoff]
            for ts in old_keys:
                del buckets[ts]
            if not buckets:
                to_delete.append(sym)
        for sym in to_delete:
            del self.data[sym]

    async def history(self, symbol: str, hours: int) -> List[Dict[str, float]]:
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

    async def symbols(self) -> List[str]:
        async with self.lock:
            return list(self.data.keys())


liq_store = LiquidationStore()
app = FastAPI(title="Haydar Liquidation Engine")

# Saatlik alarm için: symbol => en son alarm atılan bucket_ts
hourly_alert_state: Dict[str, int] = {}


# ============================
#  TELEGRAM HELPER
# ============================

async def send_telegram_message(text: str) -> None:
    """Env yoksa sessizce geç, varsa Telegram'a mesaj at."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json=payload)
    except Exception:
        # Alarm sistemi çökmemesi için hatayı yutuyoruz
        return


# ============================
#  FASTAPI ROUTES
# ============================

@app.get("/")
async def home():
    return {"status": "Backend Çalışıyor", "message": "Haydar Mode 🔥"}


@app.get("/liquidation/history")
async def get_liquidation_history(
    symbol: str = Query(..., description="Örn: BTCUSDT, ETHUSDT, SOLUSDT"),
    hours: int = Query(24, ge=1, le=24 * 7, description="Kaç saatlik geçmiş (1-168)"),
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
    hours: int = Query(24, ge=1, le=24 * 7, description="Kaç saatlik geçmiş (1-168)"),
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


# DEBUG / TEST: Telegram'a test mesajı atmak için
@app.get("/test-alert")
async def test_alert(msg: str = "Haydar test alert 🔥"):
    await send_telegram_message(f"✅ TEST ALERT\n\n{msg}")
    return {"ok": True, "sent": msg}


# ============================
#  BINANCE LIQ WORKER
# ============================

async def _handle_binance_event(event: Dict) -> None:
    """Binance !forceOrder@arr event -> store + tekil alarm kontrolü"""
    try:
        inner = event.get("o", {})
        symbol = inner.get("s")
        side = inner.get("S")  # BUY/SELL
        avg_price = float(inner.get("ap", "0") or "0")
        qty_last = float(inner.get("z", "0") or "0")
        if qty_last == 0:
            qty_last = float(inner.get("l", "0") or "0")
        ts_ms = int(inner.get("T") or event.get("E") or int(time.time() * 1000))

        if not symbol or avg_price <= 0 or qty_last <= 0 or side not in ("BUY", "SELL"):
            return

        notional = avg_price * qty_last

        # SELL = Long pozisyon likidasyonu (LONG)
        # BUY  = Short pozisyon likidasyonu (SHORT)
        logical_side = "LONG" if side == "SELL" else "SHORT"

        # Store'a kaydet
        await liq_store.add(symbol, ts_ms, logical_side, notional)

        # Büyük tekil event alarmı
        if notional >= ALERT_MIN_EVENT_USD:
            usd_str = f"{notional:,.0f}"
            msg = (
                f"💥 *BÜYÜK LİKİDASYON*\n"
                f"Symbol: `{symbol}`\n"
                f"Yön: *{logical_side}*\n"
                f"Tutar: *{usd_str} USD*\n"
                f"Borsa: Binance\n"
            )
            await send_telegram_message(msg)

    except Exception:
        return


async def binance_liquidation_worker() -> None:
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

                    if isinstance(payload, list):
                        events = payload
                    else:
                        events = [payload]

                    for ev in events:
                        if isinstance(ev, dict):
                            await _handle_binance_event(ev)
        except Exception as e:
            print(f"[Binance] Hata: {e}. 5 sn sonra yeniden denenecek: {e}")
            await asyncio.sleep(5)


# ============================
#  SAATLİK TOPLAM ALARM WORKER
# ============================

async def hourly_alert_worker() -> None:
    """
    Her 60 sn'de bir:
      - Her symbol için son 1 saatin toplam likidasyonunu hesaplar
      - ALERT_MIN_HOURLY_USD üstündeyse ve o bucket için alarm atılmadıysa Telegram'a yollar
    """
    global hourly_alert_state
    while True:
        try:
            symbols = await liq_store.symbols()
            for sym in symbols:
                points = await liq_store.history(sym, hours=1)
                if not points:
                    continue

                last = points[-1]
                bucket_ts = last["ts"]
                total = last["total"]

                if total >= ALERT_MIN_HOURLY_USD:
                    prev_bucket = hourly_alert_state.get(sym)
                    if prev_bucket != bucket_ts:
                        usd_str = f"{total:,.0f}"
                        msg = (
                            f"🕒 *SAATLİK LİKİDASYON PATLAMASI*\n"
                            f"Symbol: `{sym}`\n"
                            f"Son 1 Saat Toplam: *{usd_str} USD*\n"
                            f"Long: {last['long']:,.0f} USD\n"
                            f"Short: {last['short']:,.0f} USD\n"
                        )
                        await send_telegram_message(msg)
                        hourly_alert_state[sym] = bucket_ts
        except Exception:
            pass

        await asyncio.sleep(60)


# ============================
#  STARTUP
# ============================

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(binance_liquidation_worker())
    asyncio.create_task(hourly_alert_worker())
    print("Startup: Binance worker + hourly alert worker başlatıldı.")
