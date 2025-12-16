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

# === CORS ===
# На проде лучше поставить конкретный домен фронта вместо "*"
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.environ.get("DB_PATH", "db.sqlite3")
START_BALANCE = int(os.environ.get("START_BALANCE", "200"))

# Если задан TG_BOT_TOKEN — initData будет строго проверяться
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()

# Для локальных тестов в браузере можно разрешить guest
ALLOW_GUEST = os.environ.get("ALLOW_GUEST", "1").strip() in ("1", "true", "True", "yes", "YES")

# Максимальная "свежесть" initData (сек). Telegram присылает auth_date.
INITDATA_MAX_AGE_SEC = int(os.environ.get("INITDATA_MAX_AGE_SEC", str(24 * 3600)))

# Должно совпадать с карточками на фронте (есть id=5)
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

    # Чуть более “боевой” режим SQLite
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
        created_at INTEGER NOT NULL,
        FOREIGN KEY (tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE
      )
    """)

    cur.execute("""
      CREATE TABLE IF NOT EXISTS inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_user_id TEXT NOT NULL,
        prize_id INTEGER NOT NULL,
        prize_name TEXT NOT NULL,
        prize_cost INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        FOREIGN KEY (tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE
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


def _verify_init_data(init_data: str) -> Optional[str]:
    """
    Возвращает tg_user_id если ок.
    Если init_data пустой — может вернуть "guest" (если ALLOW_GUEST).
    Если TG_BOT_TOKEN не задан — проверка подписи пропускается (но это небезопасно).
    """
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

    # Если нет токена — работаем “как раньше”,
