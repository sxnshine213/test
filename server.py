from fastapi import FastAPI
from pydantic import BaseModel
import random
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    uid = extract_tg_user_id(req.initData)
    bal = get_or_create_user(uid)

    cost = int(req.cost or 25)
    if cost not in (25, 50):
        raise HTTPException(status_code=400, detail="bad cost")

    if bal < cost:
        raise HTTPException(status_code=402, detail="not enough balance")

    # 🔴 СПИСЫВАЕМ БАЛАНС
    new_balance = bal - cost
    set_balance(uid, new_balance)

    prize = random.choices(PRIZES, weights=[p["weight"] for p in PRIZES], k=1)[0]

    return {
        "id": prize["id"],
        "name": prize["name"],
        "cost": prize["cost"],
        "balance": new_balance   # ← ВАЖНО
    }
