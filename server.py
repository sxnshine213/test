import os
import json
import time
import random
import sqlite3
import uuid
import hmac
import hashlib
import urllib.request
from urllib.parse import parse_qsl
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # позже можно ограничить доменом фронта
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.environ.get("DB_PATH", "db.sqlite3")
START_BALANCE = int(os.environ.get("START_BALANCE", "200"))

# Токен бота (нужен для createInvoiceLink + answerPreCheckoutQuery + setWebhook)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

# Секрет для webhook (рекомендуется). При setWebhook(secret_token=...) Telegram пришлет заголовок:
# X-Telegram-Bot-Api-Secret-Token: <секрет>
TG_WEBHOOK_SECRET = os.environ.get("TG_WEBHOOK_SECRET", "").strip()

# Для локальных тестов в браузере можно разрешить guest (initData пустой)
ALLOW_GUEST = os.environ.get("ALLOW_GUEST", "1").strip() in ("1", "true", "True", "yes", "YES")

# Макс. “свежесть” initData (сек) — если BOT_TOKEN задан, будем проверять подпись
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


class TopupCreateReq(BaseModel):
    initData: str
    stars: int  # сколько Stars купить (и на сколько пополнить баланс)


def db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    # Более “боевой” режим SQLite
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

    # Пополнения Stars
    cur.execute("""
      CREATE TABLE IF NOT EXISTS topups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_user_id TEXT NOT NULL,
        payload TEXT NOT NULL UNIQUE,
        stars_amount INTEGER NOT NULL,
        status TEXT NOT NULL,                 -- created/paid
        telegram_charge_id TEXT UNIQUE,
        created_at INTEGER NOT NULL,
        paid_at INTEGER
      )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_topups_user_time ON topups(tg_user_id, created_at)")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_user_time ON spins(tg_user_id, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_user_time ON inventory(tg_user_id, created_at)")

    con.commit()
    con.close()


init_db()


def _parse_init_data(init_data: str) -> dict:
    return dict(parse_qsl(init_data, keep_blank_values=True))


def extract_tg_user_id(init_data: str) -> str:
    """
    Если BOT_TOKEN задан — проверяем подпись initData (как в Telegram Web Apps).
    Если BOT_TOKEN не задан — упрощенно достаем user.id (как раньше).
    Если initData пустой — guest (если разрешено).
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

    # Если бот-токена нет — работаем “как раньше” (небезопасно для прода)
    if not BOT_TOKEN:
        try:
            user = json.loads(user_json)
            return str(user.get("id", "guest"))
        except Exception:
            if ALLOW_GUEST:
                return "guest"
            raise HTTPException(status_code=401, detail="bad initData")

    # Проверка подписи initData (HMAC)
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

    secret_key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calc_hash, their_hash):
        raise HTTPException(status_code=401, detail="initData invalid")

    try:
        user = json.loads(user_json)
        return str(user.get("id"))
    except Exception:
        raise HTTPException(status_code=401, detail="bad user json")


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
    con.execute("UPDATE users SET balance=? WHERE tg_user_id=?", (new_balance, tg_user_id))
    con.commit()
    con.close()


def add_to_inventory(tg_user_id: str, prize_id: int, prize_name: str, prize_cost: int):
    con = db()
    con.execute(
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
    rows = con.execute(
        "SELECT prize_id, prize_name, prize_cost, created_at FROM inventory "
        "WHERE tg_user_id=? ORDER BY created_at DESC LIMIT 200",
        (uid,)
    ).fetchall()
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

    new_balance = bal - cost
    set_balance(uid, new_balance)

    prize = random.choices(PRIZES, weights=[p["weight"] for p in PRIZES], k=1)[0]
    spin_id = str(uuid.uuid4())
    now = int(time.time())

    con = db()
    con.execute(
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
        "balance": int(new_balance),
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

    add_to_inventory(uid, prize_id, prize_name, prize_cost)
    cur.execute("UPDATE spins SET status='kept' WHERE spin_id=?", (req.spin_id,))
    con.commit()
    con.close()

    bal = get_or_create_user(uid)
    return {"ok": True, "status": "kept", "balance": int(bal)}


# =========================
# Telegram Stars Top-up
# =========================

@app.post("/topup/create")
def topup_create(req: TopupCreateReq):
    """
    Создаем invoice link на XTR (Telegram Stars).
    На фронте откроем tg.openInvoice(invoice_link).
    """
    uid = extract_tg_user_id(req.initData)

    stars = int(req.stars or 0)
    if stars < 1 or stars > 10000:
        raise HTTPException(status_code=400, detail="bad stars amount")

    get_or_create_user(uid)

    payload = f"topup:{uid}:{uuid.uuid4()}"
    now = int(time.time())

    con = db()
    try:
        con.execute(
            "INSERT INTO topups (tg_user_id, payload, stars_amount, status, created_at) VALUES (?,?,?,?,?)",
            (uid, payload, stars, "created", now)
        )
        con.commit()
    finally:
        con.close()

    # Важно: для Stars валюта XTR; provider_token для цифровых товаров не нужен
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
    """
    Webhook Telegram Bot API:
    - pre_checkout_query => answerPreCheckoutQuery(ok=true)
    - successful_payment => начисление баланса
    """
    if TG_WEBHOOK_SECRET:
        got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if got != TG_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="bad webhook secret")

    update = await request.json()

    # 1) Pre-checkout: надо ответить, иначе транзакция отменится
    if "pre_checkout_query" in update:
        q = update["pre_checkout_query"]
        tg_api("answerPreCheckoutQuery", {
            "pre_checkout_query_id": q["id"],
            "ok": True
        })
        return {"ok": True}

    # 2) Successful payment: начисляем
    msg = update.get("message") or {}
    sp = msg.get("successful_payment")
    if sp:
        currency = sp.get("currency")
        total_amount = int(sp.get("total_amount", 0))
        invoice_payload = sp.get("invoice_payload", "")
        telegram_charge_id = sp.get("telegram_payment_charge_id")

        # Stars идут как XTR
        if currency != "XTR":
            return {"ok": True}

        con = db()
        try:
            row = con.execute(
                "SELECT tg_user_id, stars_amount, status FROM topups WHERE payload=?",
                (invoice_payload,)
            ).fetchone()
            if not row:
                return {"ok": True}

            if row["status"] == "paid":
                return {"ok": True}

            uid = row["tg_user_id"]
            expected = int(row["stars_amount"])

            # Для XTR ожидаем целое число Stars
            if total_amount != expected:
                return {"ok": True}

            # начисляем + отмечаем как paid (идемпотентно)
            con.execute("UPDATE users SET balance = balance + ? WHERE tg_user_id=?", (expected, uid))
            con.execute(
                "UPDATE topups SET status='paid', telegram_charge_id=?, paid_at=? WHERE payload=?",
                (telegram_charge_id, int(time.time()), invoice_payload)
            )
            con.commit()
        finally:
            con.close()

        return {"ok": True}

    return {"ok": True}
