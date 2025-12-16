import os
import json
import time
import random
import sqlite3
import uuid
from urllib.parse import parse_qsl
from typing import Literal

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

# ВАЖНО: добавил id=5 (🌹), чтобы совпадало с фронтом
PRIZES = [
    {"id": 1, "name": "❤️ Сердце", "cost": 15, "weight": 50},
    {"id": 2, "name": "🧸 Мишка", "cost": 25, "weight": 25},
    {"id": 3, "name": "🎂 Торт", "cost": 50, "weight": 15},
    {"id": 4, "name": "💎 Алмаз", "cost": 100, "weight": 10},
    {"id": 5, "name": "🌹 Роза", "cost": 25, "weight": 25},
]


class MeReq(BaseModel):
    initData: str


class SpinReq(BaseModel):
    initData: str
    cost: int = 25


class ClaimReq(BaseModel):
    initData: str
    spin_id: str
    action: Literal["sell", "keep"]


class InventoryReq(BaseModel):
    initData: str


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

    # pending-события после /spin (чтобы потом можно было выбрать sell/keep)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS spins (
        spin_id TEXT PRIMARY KEY,
        tg_user_id TEXT NOT NULL,
        bet_cost INTEGER NOT NULL,
        prize_id INTEGER NOT NULL,
        prize_name TEXT NOT NULL,
        prize_cost INTEGER NOT NULL,
        status TEXT NOT NULL,            -- pending/sold/kept
        created_at INTEGER NOT NULL
      )
    """)

    # инвентарь
    cur.execute("""
      CREATE TABLE IF NOT EXISTS inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_user_id TEXT NOT NULL,
        prize_id INTEGER NOT NULL,
        prize_name TEXT NOT NULL,
        prize_cost INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      )
    """)

    # индексы
    cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_user_time ON spins(tg_user_id, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_user_time ON inventory(tg_user_id, created_at)")

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


def add_to_inventory(tg_user_id: str, prize_id: int, prize_name: str, prize_cost: int):
    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO inventory (tg_user_id, prize_id, prize_name, prize_cost, created_at) VALUES (?,?,?,?,?)",
        (tg_user_id, int(prize_id), str(prize_name), int(prize_cost), int(time.time()))
    )
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


@app.post("/inventory")
def inventory(req: InventoryReq):
    uid = extract_tg_user_id(req.initData)
    get_or_create_user(uid)

    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT prize_id, prize_name, prize_cost, created_at FROM inventory "
        "WHERE tg_user_id=? ORDER BY created_at DESC LIMIT 200",
        (uid,)
    )
    rows = cur.fetchall()
    con.close()

    items = []
    for r in rows:
        items.append({
            "prize_id": int(r["prize_id"]),
            "prize_name": r["prize_name"],
            "prize_cost": int(r["prize_cost"]),
            "created_at": int(r["created_at"]),
        })
    return {"items": items}


@app.post("/spin")
def spin(req: SpinReq):
    uid = extract_tg_user_id(req.initData)
    bal = get_or_create_user(uid)

    cost = int(req.cost or 25)
    if cost not in (25, 50):
        raise HTTPException(status_code=400, detail="bad cost")

    if bal < cost:
        raise HTTPException(status_code=402, detail="not enough balance")

    # списываем ставку
    new_balance = bal - cost
    set_balance(uid, new_balance)

    prize = random.choices(PRIZES, weights=[p["weight"] for p in PRIZES], k=1)[0]

    spin_id = str(uuid.uuid4())
    now = int(time.time())

    con = db()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO spins (spin_id, tg_user_id, bet_cost, prize_id, prize_name, prize_cost, status, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (spin_id, uid, cost, int(prize["id"]), str(prize["name"]), int(prize["cost"]), "pending", now)
    )
    con.commit()
    con.close()

    return {
        "spin_id": spin_id,
        "id": int(prize["id"]),
        "name": str(prize["name"]),
        "cost": int(prize["cost"]),
        "balance": int(new_balance),  # баланс после списания ставки
    }


@app.post("/claim")
def claim(req: ClaimReq):
    uid = extract_tg_user_id(req.initData)
    get_or_create_user(uid)

    con = db()
    cur = con.cursor()

    cur.execute(
        "SELECT spin_id, tg_user_id, prize_id, prize_name, prize_cost, status "
        "FROM spins WHERE spin_id=? AND tg_user_id=?",
        (req.spin_id, uid)
    )
    row = cur.fetchone()
    if not row:
        con.close()
        raise HTTPException(status_code=404, detail="spin not found")

    status = row["status"]
    prize_id = int(row["prize_id"])
    prize_name = str(row["prize_name"])
    prize_cost = int(row["prize_cost"])

    # если уже обработан — просто вернём текущий баланс (идемпотентность)
    if status in ("sold", "kept"):
        con.close()
        bal = get_or_create_user(uid)
        return {"ok": True, "status": status, "balance": int(bal)}

    if req.action == "sell":
        bal = get_or_create_user(uid)
        new_balance = bal + prize_cost
        set_balance(uid, new_balance)
        cur.execute("UPDATE spins SET status='sold' WHERE spin_id=?", (req.spin_id,))
        con.commit()
        con.close()
        return {"ok": True, "status": "sold", "balance": int(new_balance), "credited": int(prize_cost)}

    # keep
    add_to_inventory(uid, prize_id, prize_name, prize_cost)
    cur.execute("UPDATE spins SET status='kept' WHERE spin_id=?", (req.spin_id,))
    con.commit()
    con.close()

    bal = get_or_create_user(uid)
    return {"ok": True, "status": "kept", "balance": int(bal)}
