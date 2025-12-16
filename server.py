import os
import json
import time
import random
import sqlite3
import uuid
import hmac
import hashlib
from urllib.parse import parse_qsl
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # на проде лучше указать домен фронта
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.environ.get("DB_PATH", "db.sqlite3")
START_BALANCE = int(os.environ.get("START_BALANCE", "200"))

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
ALLOW_GUEST = os.environ.get("ALLOW_GUEST", "1").strip() in ("1", "true", "True", "yes", "YES")
INITDATA_MAX_AGE_SEC = int(os.environ.get("INITDATA_MAX_AGE_SEC", str(24 * 3600)))

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


class PendingReq(BaseModel):
    initData: str


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=5000")
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_user_time ON spins(tg_user_id, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_user_status_time ON spins(tg_user_id, status, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_user_time ON inventory(tg_user_id, created_at)")

    con.commit()
    con.close()


init_db()


def _parse_init_data(init_data: str) -> dict:
    return dict(parse_qsl(init_data, keep_blank_values=True))


def _verify_init_data(init_data: str) -> str:
    if not init_data:
        if ALLOW_GUEST:
            return "guest"
        raise HTTPException(status_code=401, detail="initData required")

    data = _parse_init_data(init_data)
    user_json = data.get("user")
    if not user_json:
        if ALLOW_GUEST:
            return "guest"
        raise HTTPException(status_code=401, detail="no user in initData")

    # Если токен не задан — пропускаем проверку (НЕ безопасно для прода)
    if not TG_BOT_TOKEN:
        try:
            user = json.loads(user_json)
            return str(user.get("id", "guest"))
        except Exception:
            if ALLOW_GUEST:
                return "guest"
            raise HTTPException(status_code=401, detail="bad initData")

    their_hash = data.get("hash")
    if not their_hash:
        raise HTTPException(status_code=401, detail="initData hash missing")

    try:
        auth_date = int(data.get("auth_date", "0"))
    except Exception:
        auth_date = 0

    now = int(time.time())
    if not auth_date or abs(now - auth_date) > INITDATA_MAX_AGE_SEC:
        raise HTTPException(status_code=401, detail="initData expired")

    pairs = []
    for k in sorted(data.keys()):
        if k == "hash":
            continue
        pairs.append(f"{k}={data[k]}")
    data_check_string = "\n".join(pairs)

    secret_key = hashlib.sha256(TG_BOT_TOKEN.encode("utf-8")).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calc_hash, their_hash):
        raise HTTPException(status_code=401, detail="initData invalid")

    try:
        user = json.loads(user_json)
        return str(user.get("id"))
    except Exception:
        raise HTTPException(status_code=401, detail="bad user json")


def get_or_create_user(con: sqlite3.Connection, tg_user_id: str) -> int:
    row = con.execute("SELECT balance FROM users WHERE tg_user_id=?", (tg_user_id,)).fetchone()
    if row:
        return int(row["balance"])
    con.execute(
        "INSERT INTO users (tg_user_id, balance, created_at) VALUES (?,?,?)",
        (tg_user_id, START_BALANCE, int(time.time()))
    )
    return START_BALANCE


def add_to_inventory(con: sqlite3.Connection, tg_user_id: str, prize_id: int, prize_name: str, prize_cost: int):
    con.execute(
        "INSERT INTO inventory (tg_user_id, prize_id, prize_name, prize_cost, created_at) VALUES (?,?,?,?,?)",
        (tg_user_id, int(prize_id), str(prize_name), int(prize_cost), int(time.time()))
    )


@app.get("/")
def root():
    return {"ok": True}


@app.post("/me")
def me(req: MeReq):
    uid = _verify_init_data(req.initData)
    con = db()
    try:
        with con:
            bal = get_or_create_user(con, uid)
        return {"tg_user_id": uid, "balance": int(bal)}
    finally:
        con.close()


@app.post("/inventory")
def inventory(req: InventoryReq):
    uid = _verify_init_data(req.initData)
    con = db()
    try:
        with con:
            get_or_create_user(con, uid)
            rows = con.execute(
                "SELECT prize_id, prize_name, prize_cost, created_at "
                "FROM inventory WHERE tg_user_id=? ORDER BY created_at DESC LIMIT 200",
                (uid,)
            ).fetchall()
        return {
            "items": [{
                "prize_id": int(r["prize_id"]),
                "prize_name": r["prize_name"],
                "prize_cost": int(r["prize_cost"]),
                "created_at": int(r["created_at"]),
            } for r in rows]
        }
    finally:
        con.close()


@app.post("/pending")
def pending(req: PendingReq):
    uid = _verify_init_data(req.initData)
    con = db()
    try:
        with con:
            get_or_create_user(con, uid)
            row = con.execute(
                "SELECT spin_id, prize_id, prize_name, prize_cost, bet_cost, created_at "
                "FROM spins WHERE tg_user_id=? AND status='pending' ORDER BY created_at DESC LIMIT 1",
                (uid,)
            ).fetchone()
        if not row:
            return {"pending": None}
        return {"pending": {
            "spin_id": row["spin_id"],
            "id": int(row["prize_id"]),
            "name": row["prize_name"],
            "cost": int(row["prize_cost"]),
            "bet_cost": int(row["bet_cost"]),
            "created_at": int(row["created_at"]),
        }}
    finally:
        con.close()


@app.post("/spin")
def spin(req: SpinReq):
    uid = _verify_init_data(req.initData)
    cost = int(req.cost or 25)
    if cost not in (25, 50):
        raise HTTPException(status_code=400, detail="bad cost")

    con = db()
    try:
        with con:
            get_or_create_user(con, uid)

            # запрет нового спина если есть pending
            pending_row = con.execute(
                "SELECT spin_id, prize_id, prize_name, prize_cost "
                "FROM spins WHERE tg_user_id=? AND status='pending' ORDER BY created_at DESC LIMIT 1",
                (uid,)
            ).fetchone()
            if pending_row:
                bal = con.execute("SELECT balance FROM users WHERE tg_user_id=?", (uid,)).fetchone()
                return {
                    "spin_id": pending_row["spin_id"],
                    "id": int(pending_row["prize_id"]),
                    "name": str(pending_row["prize_name"]),
                    "cost": int(pending_row["prize_cost"]),
                    "balance": int(bal["balance"]) if bal else START_BALANCE,
                    "already_pending": True
                }

            # атомарно списываем
            cur = con.execute(
                "UPDATE users SET balance = balance - ? WHERE tg_user_id=? AND balance >= ?",
                (cost, uid, cost)
            )
            if cur.rowcount != 1:
                raise HTTPException(status_code=402, detail="not enough balance")

            prize = random.choices(PRIZES, weights=[p["weight"] for p in PRIZES], k=1)[0]
            spin_id = str(uuid.uuid4())
            now = int(time.time())

            con.execute(
                "INSERT INTO spins (spin_id, tg_user_id, bet_cost, prize_id, prize_name, prize_cost, status, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (spin_id, uid, cost, int(prize["id"]), str(prize["name"]), int(prize["cost"]), "pending", now)
            )

            bal = con.execute("SELECT balance FROM users WHERE tg_user_id=?", (uid,)).fetchone()

        return {
            "spin_id": spin_id,
            "id": int(prize["id"]),
            "name": str(prize["name"]),
            "cost": int(prize["cost"]),
            "balance": int(bal["balance"]) if bal else 0,
        }
    finally:
        con.close()


@app.post("/claim")
def claim(req: ClaimReq):
    uid = _verify_init_data(req.initData)
    con = db()
    try:
        with con:
            get_or_create_user(con, uid)

            row = con.execute(
                "SELECT spin_id, tg_user_id, prize_id, prize_name, prize_cost, status "
                "FROM spins WHERE spin_id=? AND tg_user_id=?",
                (req.spin_id, uid)
            ).fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="spin not found")

            status = row["status"]
            prize_id = int(row["prize_id"])
            prize_name = str(row["prize_name"])
            prize_cost = int(row["prize_cost"])

            if status in ("sold", "kept"):
                bal = con.execute("SELECT balance FROM users WHERE tg_user_id=?", (uid,)).fetchone()
                return {"ok": True, "status": status, "balance": int(bal["balance"]) if bal else 0}

            if req.action == "sell":
                con.execute("UPDATE users SET balance = balance + ? WHERE tg_user_id=?", (prize_cost, uid))
                con.execute("UPDATE spins SET status='sold' WHERE spin_id=?", (req.spin_id,))
                bal = con.execute("SELECT balance FROM users WHERE tg_user_id=?", (uid,)).fetchone()
                return {"ok": True, "status": "sold", "balance": int(bal["balance"]) if bal else 0, "credited": int(prize_cost)}

            add_to_inventory(con, uid, prize_id, prize_name, prize_cost)
            con.execute("UPDATE spins SET status='kept' WHERE spin_id=?", (req.spin_id,))
            bal = con.execute("SELECT balance FROM users WHERE tg_user_id=?", (uid,)).fetchone()
            return {"ok": True, "status": "kept", "balance": int(bal["balance"]) if bal else 0}
    finally:
        con.close()
