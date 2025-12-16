import os
import json
import time
import random
import sqlite3
from urllib.parse import parse_qsl

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # позже можно ограничить доменом
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.environ.get("DB_PATH", "db.sqlite3")
START_BALANCE = 200

PRIZES = [
    {"id": 1, "name": "❤️ Сердце", "cost": 15, "weight": 50},
    {"id": 2, "name": "🧸 Мишка", "cost": 25, "weight": 25},
    {"id": 3, "name": "🎂 Торт", "cost": 50, "weight": 15},
    {"id": 4, "name": "💎 Алмаз", "cost": 100, "weight": 10},
]


class MeReq(BaseModel):
    initData: str


class SpinReq(BaseModel):
    initData: str
    cost: int = 25


def db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = db()
    cur = con.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS users (
        tg_user_id TEXT PRIMARY KEY,
        balance INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      )
    """)
    con.commit()
    con.close()


init_db()


def extract_tg_user_id(init_data: str) -> str:
    """
    Пока без крипто-проверки: просто достаём user.id из initData.
    Если initData пустой — считаем это guest (для тестов в браузере).
    """
    if not init_data:
        return "guest"

    data = dict(parse_qsl(init_data))
    user_json = data.get("user")
    if not user_json:
        return "guest"

    try:
        user = json.loads(user_json)
        return str(user.get("id", "guest"))
    except Exception:
        return "guest"


def get_or_create_user(tg_user_id: str) -> int:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT balance FROM users WHERE tg_user_id=?", (tg_user_id,))
    row = cur.fetchone()
    if row:
        con.close()
        return int(row["balance"])

    cur.execute(
        "INSERT INTO users (tg_user_id, balance, created_at) VALUES (?,?,?)",
        (tg_user_id, START_BALANCE, int(time.time()))
    )
    con.commit()
    con.close()
    return START_BALANCE


def set_balance(tg_user_id: str, new_balance: int):
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE users SET balance=? WHERE tg_user_id=?", (new_balance, tg_user_id))
    con.commit()
    con.close()


@app.get("/")
def root():
    return {"ok": True}


@app.post("/me")
def me(req: MeReq):
    uid = extract_tg_user_id(req.initData)
    bal = get_or_create_user(uid)
    return {"tg_user_id": uid, "balance": bal}


@app.post("/spin")
def spin(req: SpinReq):
    uid = extract_tg_user_id(req.initData)
    bal = get_or_create_user(uid)

    cost = int(req.cost or 25)
    if cost not in (25, 50):
        raise HTTPException(status_code=400, detail="bad cost")

    if bal < cost:
        raise HTTPException(status_code=402, detail="not enough balance")

    # списываем
    new_balance = bal - cost
    set_balance(uid, new_balance)

    prize = random.choices(PRIZES, weights=[p["weight"] for p in PRIZES], k=1)[0]

    return {
        "id": prize["id"],
        "name": prize["name"],
        "cost": prize["cost"],
        "balance": new_balance,
    }
