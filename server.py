import os
import json
import time
import random
import uuid
import hmac
import hashlib
import urllib.request
from urllib.parse import parse_qsl
from typing import Literal, Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from psycopg_pool import ConnectionPool


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # можно ограничить доменами позже
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

ADMIN_KEY = os.environ.get("ADMIN_KEY", "").strip()

# Базовые призы — будут засидены в таблицу prizes при первом запуске, если таблица пустая
DEFAULT_PRIZES = [
    {"id": 1, "name": "❤️ Сердце", "cost": 15, "weight": 50, "icon_url": None, "is_active": True, "sort_order": 10},
    {"id": 2, "name": "🧸 Мишка", "cost": 25, "weight": 25, "icon_url": None, "is_active": True, "sort_order": 20},
    {"id": 3, "name": "🎂 Торт", "cost": 50, "weight": 15, "icon_url": None, "is_active": True, "sort_order": 30},
    {"id": 4, "name": "💎 Алмаз", "cost": 100, "weight": 10, "icon_url": None, "is_active": True, "sort_order": 40},
    {"id": 5, "name": "🌹 Роза", "cost": 25, "weight": 25, "icon_url": None, "is_active": True, "sort_order": 50},
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


class RecentWinsReq(BaseModel):
    initData: str


class TopupCreateReq(BaseModel):
    initData: str
    stars: int


class LeaderboardReq(BaseModel):
    initData: str
    limit: int = 30


class AdminAdjustReq(BaseModel):
    tg_user_id: str
    delta: int


class AdminPrizeIn(BaseModel):
    id: Optional[int] = Field(default=None, description="Если не указан, будет взят следующий свободный id")
    name: str
    cost: int = Field(ge=0)
    weight: int = Field(ge=0)
    icon_url: Optional[str] = None
    is_active: bool = True
    sort_order: int = 0


# ===== DB init =====
def init_db():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
          tg_user_id TEXT PRIMARY KEY,
          balance INTEGER NOT NULL,
          created_at BIGINT NOT NULL
        )
        """,
        # публичные поля Telegram-профиля (чтобы показывать имя/аватар в топе/ленте)
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_url TEXT",
        """
        CREATE TABLE IF NOT EXISTS prizes (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          cost INTEGER NOT NULL,
          weight INTEGER NOT NULL,
          icon_url TEXT,
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          sort_order INTEGER NOT NULL DEFAULT 0,
          created_at BIGINT NOT NULL,
          updated_at BIGINT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS spins (
          spin_id TEXT PRIMARY KEY,
          tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
          bet_cost INTEGER NOT NULL,
          prize_id INTEGER NOT NULL,
          prize_name TEXT NOT NULL,
          prize_cost INTEGER NOT NULL,
          status TEXT NOT NULL,
          created_at BIGINT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS inventory (
          id BIGSERIAL PRIMARY KEY,
          tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
          prize_id INTEGER NOT NULL,
          prize_name TEXT NOT NULL,
          prize_cost INTEGER NOT NULL,
          created_at BIGINT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS topups (
          id BIGSERIAL PRIMARY KEY,
          tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
          payload TEXT NOT NULL UNIQUE,
          stars_amount INTEGER NOT NULL,
          status TEXT NOT NULL,
          telegram_charge_id TEXT UNIQUE,
          created_at BIGINT NOT NULL,
          paid_at BIGINT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_spins_user_time ON spins(tg_user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_spins_time ON spins(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_inv_user_time ON inventory(tg_user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_topups_user_time ON topups(tg_user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_prizes_sort ON prizes(is_active, sort_order, id)",
    ]

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                for st in statements:
                    cur.execute(st)

                # seed prizes if empty
                cur.execute("SELECT COUNT(*) FROM prizes")
                cnt = int(cur.fetchone()[0])
                if cnt == 0:
                    now = int(time.time())
                    for p in DEFAULT_PRIZES:
                        cur.execute(
                            "INSERT INTO prizes (id,name,cost,weight,icon_url,is_active,sort_order,created_at,updated_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (p["id"], p["name"], p["cost"], p["weight"], p["icon_url"], p["is_active"], p["sort_order"], now, now),
                        )


init_db()


# ===== Admin auth =====
def require_admin(request: Request):
    if not ADMIN_KEY:
        raise HTTPException(status_code=503, detail="ADMIN_KEY not set")
    got = request.headers.get("X-Admin-Key", "")
    if not got or not hmac.compare_digest(got, ADMIN_KEY):
        raise HTTPException(status_code=401, detail="admin unauthorized")


# ===== Telegram initData verify (WebApp) =====
def _parse_init_data(init_data: str) -> Dict[str, str]:
    return dict(parse_qsl(init_data, keep_blank_values=True))


def _extract_user_dict(init_data: str) -> Optional[Dict[str, Any]]:
    if not init_data:
        return None
    data = _parse_init_data(init_data)
    user_json = data.get("user")
    if not user_json:
        return None
    try:
        return json.loads(user_json)
    except Exception:
        return None


def extract_tg_user_id(init_data: str) -> str:
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

    # fallback (только для дебага)
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
def tg_api(method: str, payload: dict):
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


def upsert_profile(cur, tg_user_id: str, user: Optional[Dict[str, Any]]):
    if not user:
        return
    username = user.get("username")
    first_name = user.get("first_name")
    last_name = user.get("last_name")
    photo_url = user.get("photo_url")
    cur.execute(
        """
        UPDATE users
        SET username = COALESCE(%s, username),
            first_name = COALESCE(%s, first_name),
            last_name = COALESCE(%s, last_name),
            photo_url = COALESCE(%s, photo_url)
        WHERE tg_user_id=%s
        """,
        (username, first_name, last_name, photo_url, tg_user_id),
    )


def display_name_from_row(username: Optional[str], first_name: Optional[str], last_name: Optional[str], tg_user_id: str) -> str:
    if username:
        u = str(username).strip()
        if u:
            return "@" + u.lstrip("@")
    full = (" ".join([str(first_name or "").strip(), str(last_name or "").strip()])).strip()
    if full:
        return full
    tail = tg_user_id[-4:] if len(tg_user_id) >= 4 else tg_user_id
    return f"User {tail}"


def pick_prize_from_db(cur) -> Dict[str, Any]:
    cur.execute(
        "SELECT id, name, cost, weight, icon_url FROM prizes WHERE is_active=TRUE AND weight>0 ORDER BY sort_order ASC, id ASC"
    )
    rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=503, detail="no active prizes")
    prizes = []
    weights = []
    for r in rows:
        prizes.append({"id": int(r[0]), "name": str(r[1]), "cost": int(r[2]), "weight": int(r[3]), "icon_url": r[4]})
        weights.append(int(r[3]))
    return random.choices(prizes, weights=weights, k=1)[0]


@app.get("/")
def root():
    return {"ok": True}


@app.get("/prizes")
def prizes_public():
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT id, name, cost, icon_url, sort_order FROM prizes WHERE is_active=TRUE ORDER BY sort_order ASC, id ASC"
                )
                rows = cur.fetchall()
    return {"items": [{"id": int(r[0]), "name": str(r[1]), "cost": int(r[2]), "icon_url": (r[3] or None), "sort_order": int(r[4])} for r in rows]}


@app.post("/me")
def me(req: MeReq):
    uid = extract_tg_user_id(req.initData)
    user = _extract_user_dict(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                bal = get_or_create_user(cur, uid)
                upsert_profile(cur, uid, user)
    return {"tg_user_id": uid, "balance": int(bal)}


@app.post("/inventory")
def inventory(req: InventoryReq):
    uid = extract_tg_user_id(req.initData)
    user = _extract_user_dict(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)
                upsert_profile(cur, uid, user)

                cur.execute(
                    """
                    SELECT i.prize_id, i.prize_name, i.prize_cost, i.created_at, p.icon_url
                    FROM inventory i
                    LEFT JOIN prizes p ON p.id = i.prize_id
                    WHERE i.tg_user_id=%s
                    ORDER BY i.created_at DESC
                    LIMIT 200
                    """,
                    (uid,),
                )
                rows = cur.fetchall()

    return {"items": [{
        "prize_id": int(r[0]),
        "prize_name": r[1],
        "prize_cost": int(r[2]),
        "created_at": int(r[3]),
        "icon_url": (r[4] or None),
    } for r in rows]}


@app.post("/spin")
def spin(req: SpinReq):
    uid = extract_tg_user_id(req.initData)
    user = _extract_user_dict(req.initData)

    cost = int(req.cost or 25)
    if cost not in (25, 50):
        raise HTTPException(status_code=400, detail="bad cost")

    spin_id = str(uuid.uuid4())
    now = int(time.time())

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)
                upsert_profile(cur, uid, user)

                # списываем ставку
                cur.execute(
                    "UPDATE users SET balance = balance - %s "
                    "WHERE tg_user_id=%s AND balance >= %s "
                    "RETURNING balance",
                    (cost, uid, cost),
                )
                row = cur.fetchone()
                if not row:
                    # 402 = недостаточно средств
                    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (uid,))
                    bal = int(cur.fetchone()[0])
                    raise HTTPException(status_code=402, detail=f"not enough balance (balance={bal}, cost={cost})")
                new_balance = int(row[0])

                prize = pick_prize_from_db(cur)

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
        "icon_url": (prize.get("icon_url") or None),
        "balance": int(new_balance),
    }


@app.post("/claim")
def claim(req: ClaimReq):
    uid = extract_tg_user_id(req.initData)
    user = _extract_user_dict(req.initData)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)
                upsert_profile(cur, uid, user)

                cur.execute(
                    "SELECT prize_id, prize_name, prize_cost, status "
                    "FROM spins WHERE spin_id=%s AND tg_user_id=%s FOR UPDATE",
                    (req.spin_id, uid),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="spin not found")

                prize_id, prize_name, prize_cost, status = int(row[0]), str(row[1]), int(row[2]), str(row[3])

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


@app.post("/recent_wins")
def recent_wins(req: RecentWinsReq, limit: int = Query(20, ge=1, le=50)):
    # auth нужен, чтобы не было 401 в MiniApp (та же схема, что и /me)
    _ = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.tg_user_id, u.username, u.first_name, u.last_name, u.photo_url,
                           s.prize_id, s.prize_name, s.prize_cost, s.created_at,
                           p.icon_url
                    FROM spins s
                    JOIN users u ON u.tg_user_id = s.tg_user_id
                    LEFT JOIN prizes p ON p.id = s.prize_id
                    ORDER BY s.created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()

    items = []
    for r in rows:
        tg_user_id = str(r[0])
        name = display_name_from_row(r[1], r[2], r[3], tg_user_id)
        avatar = r[4] or None
        items.append({
            "tg_user_id": tg_user_id,
            "name": name,
            "avatar": avatar,
            "prize_id": int(r[5]),
            "prize_name": str(r[6]),
            "prize_cost": int(r[7]),
            "created_at": int(r[8]),
            "icon_url": (r[9] or None),
        })
    return {"items": items}


@app.post("/leaderboard")
def leaderboard(req: LeaderboardReq):
    uid = extract_tg_user_id(req.initData)
    user = _extract_user_dict(req.initData)
    limit = max(5, min(100, int(req.limit or 30)))

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                my_balance = get_or_create_user(cur, uid)
                upsert_profile(cur, uid, user)

                cur.execute(
                    "SELECT tg_user_id, balance, username, first_name, last_name, photo_url "
                    "FROM users ORDER BY balance DESC, created_at ASC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()

                cur.execute("SELECT 1 + COUNT(*) FROM users WHERE balance > %s", (my_balance,))
                my_rank = int(cur.fetchone()[0])

    items = []
    for i, r in enumerate(rows, start=1):
        tuid = str(r[0])
        items.append({
            "rank": i,
            "tg_user_id": tuid,
            "name": display_name_from_row(r[2], r[3], r[4], tuid),
            "avatar": (r[5] or None),
            "balance": int(r[1]),
            "is_me": tuid == str(uid),
        })

    return {"items": items, "me": {"rank": my_rank, "balance": int(my_balance)}}


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

    if "pre_checkout_query" in update:
        q = update["pre_checkout_query"]
        tg_api("answerPreCheckoutQuery", {"pre_checkout_query_id": q["id"], "ok": True})
        return {"ok": True}

    msg = update.get("message") or {}
    sp = msg.get("successful_payment")
    if sp:
        if sp.get("currency") != "XTR":
            return {"ok": True}

        total_amount = int(sp.get("total_amount", 0))
        invoice_payload = sp.get("invoice_payload", "")
        telegram_charge_id = sp.get("telegram_payment_charge_id")

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

                    cur.execute("UPDATE users SET balance = balance + %s WHERE tg_user_id=%s", (expected, uid))
                    cur.execute(
                        "UPDATE topups SET status='paid', telegram_charge_id=%s, paid_at=%s WHERE payload=%s",
                        (telegram_charge_id, int(time.time()), invoice_payload),
                    )

        return {"ok": True}

    return {"ok": True}


# ===== Admin endpoints =====
@app.get("/admin/stats")
def admin_stats(request: Request):
    require_admin(request)
    now = int(time.time())
    day_ago = now - 86400

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                users = int(cur.fetchone()[0])

                cur.execute("SELECT COALESCE(SUM(balance),0) FROM users")
                total_balance = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM spins")
                spins_total = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM spins WHERE created_at >= %s", (day_ago,))
                spins_24h = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM topups")
                topups_total = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM topups WHERE created_at >= %s", (day_ago,))
                topups_24h = int(cur.fetchone()[0])

                cur.execute("SELECT COALESCE(SUM(stars_amount),0) FROM topups WHERE status='paid'")
                paid_stars_total = int(cur.fetchone()[0])

                cur.execute("SELECT COALESCE(SUM(stars_amount),0) FROM topups WHERE status='paid' AND paid_at >= %s", (day_ago,))
                paid_stars_24h = int(cur.fetchone()[0])

    return {
        "users": users,
        "total_balance": total_balance,
        "spins_total": spins_total,
        "spins_24h": spins_24h,
        "topups_total": topups_total,
        "topups_24h": topups_24h,
        "paid_stars_total": paid_stars_total,
        "paid_stars_24h": paid_stars_24h,
    }


@app.get("/admin/topups")
def admin_topups(request: Request, limit: int = Query(80, ge=1, le=500)):
    require_admin(request)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT tg_user_id, payload, stars_amount, status, telegram_charge_id, created_at, paid_at "
                    "FROM topups ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()

    items = []
    for r in rows:
        items.append({
            "tg_user_id": r[0],
            "payload": r[1],
            "stars_amount": int(r[2]),
            "status": r[3],
            "telegram_charge_id": r[4],
            "created_at": int(r[5]),
            "paid_at": int(r[6]) if r[6] else None,
        })
    return {"items": items}


@app.get("/admin/user/{tg_user_id}")
def admin_user(request: Request, tg_user_id: str):
    require_admin(request)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT tg_user_id, balance, created_at, username, first_name, last_name, photo_url "
                    "FROM users WHERE tg_user_id=%s",
                    (tg_user_id,),
                )
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=404, detail="user not found")

                cur.execute(
                    "SELECT spin_id, bet_cost, prize_id, prize_name, prize_cost, status, created_at "
                    "FROM spins WHERE tg_user_id=%s ORDER BY created_at DESC LIMIT 30",
                    (tg_user_id,),
                )
                spins = cur.fetchall()

                cur.execute(
                    "SELECT prize_id, prize_name, prize_cost, created_at "
                    "FROM inventory WHERE tg_user_id=%s ORDER BY created_at DESC LIMIT 30",
                    (tg_user_id,),
                )
                inv = cur.fetchall()

                cur.execute(
                    "SELECT payload, stars_amount, status, created_at, paid_at "
                    "FROM topups WHERE tg_user_id=%s ORDER BY created_at DESC LIMIT 30",
                    (tg_user_id,),
                )
                topups = cur.fetchall()

    return {
        "user": {
            "tg_user_id": u[0],
            "balance": int(u[1]),
            "created_at": int(u[2]),
            "name": display_name_from_row(u[3], u[4], u[5], str(u[0])),
            "avatar": (u[6] or None),
        },
        "spins": [{
            "spin_id": s[0],
            "bet_cost": int(s[1]),
            "prize_id": int(s[2]),
            "prize_name": s[3],
            "prize_cost": int(s[4]),
            "status": s[5],
            "created_at": int(s[6]),
        } for s in spins],
        "inventory": [{
            "prize_id": int(i[0]),
            "prize_name": i[1],
            "prize_cost": int(i[2]),
            "created_at": int(i[3]),
        } for i in inv],
        "topups": [{
            "payload": t[0],
            "stars_amount": int(t[1]),
            "status": t[2],
            "created_at": int(t[3]),
            "paid_at": int(t[4]) if t[4] else None,
        } for t in topups],
    }


@app.post("/admin/adjust_balance")
def admin_adjust_balance(request: Request, req: AdminAdjustReq):
    require_admin(request)

    uid = str(req.tg_user_id)
    delta = int(req.delta)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, uid)
                cur.execute(
                    "UPDATE users SET balance = GREATEST(0, balance + %s) WHERE tg_user_id=%s RETURNING balance",
                    (delta, uid),
                )
                bal = int(cur.fetchone()[0])

    return {"ok": True, "tg_user_id": uid, "balance": bal, "delta": delta}


@app.get("/admin/prizes")
def admin_list_prizes(request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT id,name,cost,weight,icon_url,is_active,sort_order,updated_at "
                    "FROM prizes ORDER BY sort_order ASC, id ASC"
                )
                rows = cur.fetchall()
    items = []
    for r in rows:
        items.append({
            "id": int(r[0]),
            "name": str(r[1]),
            "cost": int(r[2]),
            "weight": int(r[3]),
            "icon_url": (r[4] or None),
            "is_active": bool(r[5]),
            "sort_order": int(r[6]),
            "updated_at": int(r[7]),
        })
    return {"items": items}


@app.post("/admin/prizes")
def admin_create_prize(request: Request, p: AdminPrizeIn):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                pid = p.id
                if pid is None:
                    cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM prizes")
                    pid = int(cur.fetchone()[0])
                cur.execute(
                    "INSERT INTO prizes (id,name,cost,weight,icon_url,is_active,sort_order,created_at,updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (int(pid), p.name, int(p.cost), int(p.weight), (p.icon_url or None), bool(p.is_active), int(p.sort_order), now, now),
                )
    return {"ok": True, "id": int(pid)}


@app.put("/admin/prizes/{prize_id}")
def admin_update_prize(request: Request, prize_id: int, p: AdminPrizeIn):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT 1 FROM prizes WHERE id=%s", (int(prize_id),))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="prize not found")
                cur.execute(
                    """
                    UPDATE prizes
                    SET name=%s, cost=%s, weight=%s, icon_url=%s, is_active=%s, sort_order=%s, updated_at=%s
                    WHERE id=%s
                    """,
                    (p.name, int(p.cost), int(p.weight), (p.icon_url or None), bool(p.is_active), int(p.sort_order), now, int(prize_id)),
                )
    return {"ok": True}


@app.delete("/admin/prizes/{prize_id}")
def admin_delete_prize(request: Request, prize_id: int):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("DELETE FROM prizes WHERE id=%s", (int(prize_id),))
    return {"ok": True}
