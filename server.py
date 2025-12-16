from fastapi import FastAPI
from pydantic import BaseModel
import random

app = FastAPI()


class SpinReq(BaseModel):
    initData: str

PRIZES = [
    {"id": 1, "name": "❤️ Сердце", "cost": 15, "weight": 50},
    {"id": 2, "name": "🧸 Мишка", "cost": 25, "weight": 25},
    {"id": 3, "name": "🎂 Торт", "cost": 50, "weight": 15},
    {"id": 4, "name": "💎 Алмаз", "cost": 100, "weight": 10},
]
@app.get("/")
def root():
    return {"ok": True}


@app.post("/spin")
def spin(req: SpinReq):
    prize = random.choices(
        PRIZES,
        weights=[p["weight"] for p in PRIZES],
        k=1
    )[0]
    return prize
