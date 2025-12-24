from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"status": "Backend Çalışıyor", "message": "Haydar Mode 🔥"}
