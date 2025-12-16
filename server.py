import os
import json
import time
import random
import uuid
import hmac
import hashlib
import urllib.request
from urllib.parse import parse_qsl
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from psycopg_pool import ConnectionPool


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # на проде лучше ограничить доменом фронта
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== ENV =====
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set (Render Postgres)")

START_BALANCE = int(os.environ.get("START_BALANCE", "200"))

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
TG_WEBHOOK_SECRET = os.environ.get("TG_WEBHOOK_SECRET", "").strip()

ALLOW_GUEST = os.environ.get("ALLOW_GUEST", "0").strip() in ("1", "true", "True", "yes", "YES")
INITDATA_MAX_AGE_SEC = int(os.environ.get("INITDATA_MAX_AGE_SEC", str(24 * 3600)))

PG_POOL_MIN = int(os.environ.get("PG_POOL_MIN", "1"))
PG_POOL_MAX = int(os.environ.get("PG_POOL_MAX", "10"))

# Должно совпадать с карточками на фронте (есть id=5)
PRIZES = [
    {"id": 1, "name": "❤️ Сердце", "cost": 15, "weight": 50},
    {"id": 2, "name": "🧸 Мишка", "cost": 25, "weight": 25},
    {"id": 3, "name": "🎂 Торт", "cost": 50, "weight": 15},
    {"id": 4, "name": "💎 Алмаз", "cost": 100, "weight": 10},
    {"id": 5, "name": "🌹 Роза", "cost": 25, "weight": 25},
]

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=PG_POOL_MIN, max_size=PG_POOL_MAX, timeout=10)


@app.on_event("shutdown")
def _shutdown():
    try:
        pool.close()
    except Exception:
        pass


# ===== Models =====
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


class TopupCreateReq(BaseModel):
    initData: str
    stars: int


# ===== DB init =====
def init_db():
    ddl = """
    CREATE TABLE IF NOT EXISTS users (
      tg_user_id TEXT PRIMARY KEY,
      balance INTEGER NOT NULL,
      created_at BIGINT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS spins (
      spin_id TEXT PRIMARY KEY,
      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
      bet_cost INTEGER NOT NULL,
      prize_id INTEGER NOT NULL,
      prize_name TEXT NOT NULL,
      prize_cost INTEGER NOT NULL,
      status TEXT NOT NULL, -- pending/sold/kept
      created_at BIGINT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS inventory (
      id BIGSERIAL PRIMARY KEY,
      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
      prize_id INTEGER NOT NULL,
      prize_name TEXT NOT NULL,
      prize_cost INTEGER NOT NULL,
      created_at BIGINT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS topups (
      id BIGSERIAL PRIMARY KEY,
      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
      payload TEXT NOT NULL UNIQUE,
      stars_amount INTEGER NOT NULL,
      status TEXT NOT NULL,                 -- created/paid
      telegram_charge_id TEXT UNIQUE,
      created_at BIGINT NOT NULL,
      paid_at BIGINT
    );

    CREATE INDEX IF NOT EXISTS idx_spins_user_time ON spins(tg_user_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_inv_user_time ON inventory(tg_user_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_topups_user_time ON topups(tg_user_id, created_at);
    """
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(ddl)


init_db()


# ===== Telegram initData =====
def _parse_init_data(init_data: str) -> dict:
    return dict(parse_qsl(init_data, keep_blank_values=True))


def extract_tg_user_id(init_data: str) -> str:
    """
    Если BOT_TOKEN задан — проверяем подпись initData (рекомендуется на проде).
    Если BOT_TOKEN не задан — упрощенно парсим user.id (НЕ рекомендуется для прода).
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

    # fallback режим без проверки подписи (только для дебага)
    if not BOT_TOKEN:
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

    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calc_hash, their_hash):
        raise HTTPException(status_code=401, detail="initData invalid")

    try:
        user = json.loads(user_json)
        return str(user.get("id"))
    except Exception:
        raise HTTPException(status_code=401, detail="bad user json")


# ===== Telegram Bot API helper (Stars) =====
def tg_api(method: str, payload: dict) -> dict:
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not set")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            obj = json.loads(raw)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"telegram api error: {e}")

    if not obj.get("ok"):
        raise HTTPException(status_code=502, detail=f"telegram api not ok: {obj}")
    return obj["result"]


# ===== DB helpers =====
def get_or_create_user(cur, tg_user_id: str) -> int:
    cur.execute(
        "INSERT INTO users (tg_user_id, balance, created_at) "
        "VALUES (%s, %s, %s) ON CONFLICT (tg_user_id) DO NOTHING",
        (tg_user_id, START_BALANCE, int(time.time())),
    )
    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (tg_user_id,))
    row = cur.fetchone()
    return int(row[0]) if row else START_BALANCE


@app.get("/")
def root():
    return {"ok": True}


@app.post("/me")
def me(req: MeReq):
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                bal = get_or_create_user(cur, uid)
    return {"tg_user_id": uid, "balance": int(bal)}


@app.post("/inventory")
def inventory(req: InventoryReq):
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)
                cur.execute(
                    "SELECT prize_id, prize_name, prize_cost, created_at "
                    "FROM inventory WHERE tg_user_id=%s "
                    "ORDER BY created_at DESC LIMIT 200",
                    (uid,),
                )
                rows = cur.fetchall()

    items = [{
        "prize_id": int(r[0]),
        "prize_name": r[1],
        "prize_cost": int(r[2]),
        "created_at": int(r[3]),
    } for r in rows]
    return {"items": items}


@app.post("/spin")
def spin(req: SpinReq):
    uid = extract_tg_user_id(req.initData)

    cost = int(req.cost or 25)
    if cost not in (25, 50):
        raise HTTPException(status_code=400, detail="bad cost")

    prize = random.choices(PRIZES, weights=[p["weight"] for p in PRIZES], k=1)[0]
    spin_id = str(uuid.uuid4())
    now = int(time.time())

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)

                # атомарное списание
                cur.execute(
                    "UPDATE users SET balance = balance - %s "
                    "WHERE tg_user_id=%s AND balance >= %s "
                    "RETURNING balance",
                    (cost, uid, cost),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=402, detail="not enough balance")
                new_balance = int(row[0])

                cur.execute(
                    "INSERT INTO spins (spin_id, tg_user_id, bet_cost, prize_id, prize_name, prize_cost, status, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'pending',%s)",
                    (spin_id, uid, cost, int(prize["id"]), str(prize["name"]), int(prize["cost"]), now),
                )

    return {
        "spin_id": spin_id,
        "id": int(prize["id"]),
        "name": str(prize["name"]),
        "cost": int(prize["cost"]),
        "balance": int(new_balance),
    }


@app.post("/claim")
def claim(req: ClaimReq):
    uid = extract_tg_user_id(req.initData)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)

                # блокируем спин, чтобы не было гонок
                cur.execute(
                    "SELECT prize_id, prize_name, prize_cost, status "
                    "FROM spins WHERE spin_id=%s AND tg_user_id=%s FOR UPDATE",
                    (req.spin_id, uid),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="spin not found")

                prize_id, prize_name, prize_cost, status = int(row[0]), str(row[1]), int(row[2]), str(row[3])

                # идемпотентность
                if status in ("sold", "kept"):
                    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (uid,))
                    bal = int(cur.fetchone()[0])
                    return {"ok": True, "status": status, "balance": bal}

                if req.action == "sell":
                    cur.execute(
                        "UPDATE users SET balance = balance + %s WHERE tg_user_id=%s RETURNING balance",
                        (prize_cost, uid),
                    )
                    bal = int(cur.fetchone()[0])
                    cur.execute("UPDATE spins SET status='sold' WHERE spin_id=%s", (req.spin_id,))
                    return {"ok": True, "status": "sold", "balance": bal, "credited": prize_cost}

                # keep
                cur.execute(
                    "INSERT INTO inventory (tg_user_id, prize_id, prize_name, prize_cost, created_at) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (uid, prize_id, prize_name, prize_cost, int(time.time())),
                )
                cur.execute("UPDATE spins SET status='kept' WHERE spin_id=%s", (req.spin_id,))
                cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (uid,))
                bal = int(cur.fetchone()[0])
                return {"ok": True, "status": "kept", "balance": bal}


# =========================
# Telegram Stars Top-up
# =========================
@app.post("/topup/create")
def topup_create(req: TopupCreateReq):
    uid = extract_tg_user_id(req.initData)

    stars = int(req.stars or 0)
    if stars < 1 or stars > 10000:
        raise HTTPException(status_code=400, detail="bad stars amount")

    payload = f"topup:{uid}:{uuid.uuid4()}"
    now = int(time.time())

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)
                cur.execute(
                    "INSERT INTO topups (tg_user_id, payload, stars_amount, status, created_at) "
                    "VALUES (%s,%s,%s,'created',%s)",
                    (uid, payload, stars, now),
                )

    invoice_link = tg_api("createInvoiceLink", {
        "title": "Пополнение баланса",
        "description": f"+{stars} ⭐ в игре",
        "payload": payload,
        "currency": "XTR",
        "prices": [{"label": f"+{stars} ⭐", "amount": stars}],
    })

    return {"invoice_link": invoice_link, "payload": payload}


@app.post("/tg/webhook")
async def tg_webhook(request: Request):
    if TG_WEBHOOK_SECRET:
        got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if got != TG_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="bad webhook secret")

    update = await request.json()

    # pre_checkout_query: надо быстро подтвердить
    if "pre_checkout_query" in update:
        q = update["pre_checkout_query"]
        tg_api("answerPreCheckoutQuery", {"pre_checkout_query_id": q["id"], "ok": True})
        return {"ok": True}

    msg = update.get("message") or {}
    sp = msg.get("successful_payment")
    if sp:
        currency = sp.get("currency")
        total_amount = int(sp.get("total_amount", 0))
        invoice_payload = sp.get("invoice_payload", "")
        telegram_charge_id = sp.get("telegram_payment_charge_id")

        if currency != "XTR":
            return {"ok": True}

        with pool.connection() as con:
            with con:
                with con.cursor() as cur:
                    cur.execute(
                        "SELECT tg_user_id, stars_amount, status FROM topups WHERE payload=%s FOR UPDATE",
                        (invoice_payload,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return {"ok": True}

                    uid, expected, status = str(row[0]), int(row[1]), str(row[2])

                    if status == "paid":
                        return {"ok": True}

                    if total_amount != expected:
                        return {"ok": True}

                    cur.execute(
                        "UPDATE users SET balance = balance + %s WHERE tg_user_id=%s",
                        (expected, uid),
                    )
                    cur.execute(
                        "UPDATE topups SET status='paid', telegram_charge_id=%s, paid_at=%s WHERE payload=%s",
                        (telegram_charge_id, int(time.time()), invoice_payload),
                    )

        return {"ok": True}

    return {"ok": True}

