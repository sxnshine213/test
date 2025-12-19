import os
import json
import time
import random
import uuid
import hmac
import hashlib
import urllib.request
from urllib.parse import parse_qsl
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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

# дефолтные призы (для первичного seed таблицы prizes, если она пустая)
DEFAULT_PRIZES = [
    {"id": 1, "name": "❤️ Сердце", "cost": 15, "weight": 50, "sort_order": 10, "is_active": True},
    {"id": 2, "name": "🧸 Мишка", "cost": 25, "weight": 25, "sort_order": 20, "is_active": True},
    {"id": 3, "name": "🎂 Торт", "cost": 50, "weight": 15, "sort_order": 30, "is_active": True},
    {"id": 4, "name": "💎 Алмаз", "cost": 100, "weight": 10, "sort_order": 40, "is_active": True},
    {"id": 5, "name": "🌹 Роза", "cost": 25, "weight": 25, "sort_order": 50, "is_active": True},
]

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=PG_POOL_MIN, max_size=PG_POOL_MAX, timeout=10)


@app.on_event("shutdown")
def _shutdown():
    try:
        pool.close()
    except Exception:
        pass


# ===== Models =====
class WithInitData(BaseModel):
    initData: str = ""


class MeReq(WithInitData):
    pass


class SpinReq(WithInitData):
    # Preferred: spin a specific case
    case_id: Optional[int] = None
    # Backward compatibility (old 25/50 pills)
    cost: Optional[int] = None


class ClaimReq(WithInitData):
    spin_id: str
    action: Literal["sell", "keep"]


class InventoryReq(WithInitData):
    pass


class InventorySellReq(WithInitData):
    inventory_id: int


class InventoryWithdrawReq(WithInitData):
    inventory_id: int


class TopupCreateReq(WithInitData):
    stars: int


class LeaderboardReq(WithInitData):
    limit: int = 30


class AdminAdjustReq(BaseModel):
    tg_user_id: str
    delta: int


class PrizeIn(BaseModel):
    name: str
    icon_url: Optional[str] = None
    cost: int
    weight: int
    # Telegram Gift id for regular gifts (used by sendGift)
    gift_id: Optional[str] = None
    # Unique gifts are handled via admin claims (manual fulfillment)
    is_unique: bool = False
    is_active: bool = True
    sort_order: int = 0


class PrizeOut(PrizeIn):
    id: int
    created_at: int


class CaseIn(BaseModel):
    name: str
    description: Optional[str] = None
    price: int
    is_active: bool = True
    sort_order: int = 0


class CaseOut(CaseIn):
    id: int
    created_at: int


class CasePrizeIn(BaseModel):
    prize_id: int
    weight: int
    is_active: bool = True


# ===== DB init =====

def init_db():
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                # users
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                      tg_user_id TEXT PRIMARY KEY,
                      balance INTEGER NOT NULL,
                      created_at BIGINT NOT NULL
                    )
                    """
                )
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_url TEXT")

                # prizes
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS prizes (
                      id BIGINT PRIMARY KEY,
                      name TEXT NOT NULL,
                      icon_url TEXT,
                      cost INTEGER NOT NULL,
                      weight INTEGER NOT NULL,
                      is_active BOOLEAN NOT NULL DEFAULT TRUE,
                      sort_order INTEGER NOT NULL DEFAULT 0,
                      created_at BIGINT NOT NULL
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_prizes_active_sort ON prizes(is_active, sort_order, id)")
                cur.execute("ALTER TABLE prizes ADD COLUMN IF NOT EXISTS icon_url TEXT")

                # spins / inventory / topups
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS spins (
                      spin_id TEXT PRIMARY KEY,
                      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
                      bet_cost INTEGER NOT NULL,
                      prize_id BIGINT NOT NULL,
                      prize_name TEXT NOT NULL,
                      prize_cost INTEGER NOT NULL,
                      status TEXT NOT NULL,
                      created_at BIGINT NOT NULL
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS inventory (
                      id BIGSERIAL PRIMARY KEY,
                      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
                      prize_id BIGINT NOT NULL,
                      prize_name TEXT NOT NULL,
                      prize_cost INTEGER NOT NULL,
                      created_at BIGINT NOT NULL
                    )
                    """
                )
                cur.execute(
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
                    """
                )

                
                # --- Schema upgrades (cases, gifts, claims, withdraw locks) ---
                cur.execute("ALTER TABLE prizes ADD COLUMN IF NOT EXISTS gift_id TEXT")
                cur.execute("ALTER TABLE prizes ADD COLUMN IF NOT EXISTS is_unique BOOLEAN NOT NULL DEFAULT FALSE")

                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_id BIGINT")
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_name TEXT")
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_price INTEGER")

                cur.execute("ALTER TABLE inventory ADD COLUMN IF NOT EXISTS is_locked BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE inventory ADD COLUMN IF NOT EXISTS locked_reason TEXT")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cases (
                      id BIGSERIAL PRIMARY KEY,
                      name TEXT NOT NULL,
                      description TEXT,
                      price INTEGER NOT NULL,
                      is_active BOOLEAN NOT NULL DEFAULT TRUE,
                      sort_order INTEGER NOT NULL DEFAULT 0,
                      created_at BIGINT NOT NULL
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cases_active_sort ON cases(is_active, sort_order, id)")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS case_prizes (
                      case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
                      prize_id BIGINT NOT NULL REFERENCES prizes(id) ON DELETE CASCADE,
                      weight INTEGER NOT NULL,
                      is_active BOOLEAN NOT NULL DEFAULT TRUE,
                      created_at BIGINT NOT NULL,
                      PRIMARY KEY (case_id, prize_id)
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_case_prizes_case ON case_prizes(case_id, is_active)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_case_prizes_prize ON case_prizes(prize_id)")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS claims (
                      id BIGSERIAL PRIMARY KEY,
                      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
                      inventory_id BIGINT NOT NULL REFERENCES inventory(id) ON DELETE CASCADE,
                      prize_id BIGINT NOT NULL,
                      prize_name TEXT NOT NULL,
                      status TEXT NOT NULL,
                      created_at BIGINT NOT NULL,
                      processed_at BIGINT
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_claims_status_time ON claims(status, created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_user_time ON spins(tg_user_id, created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_time ON spins(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_user_time ON inventory(tg_user_id, created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_topups_user_time ON topups(tg_user_id, created_at)")

                # seed prizes if empty
                cur.execute("SELECT COUNT(*) FROM prizes")
                cnt = int(cur.fetchone()[0] or 0)
                if cnt == 0:
                    now = int(time.time())
                    for p in DEFAULT_PRIZES:
                        cur.execute(
                            "INSERT INTO prizes (id, name, icon_url, cost, weight, is_active, sort_order, created_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                int(p["id"]),
                                str(p["name"]),
                                (p.get("icon_url") or None),
                                int(p["cost"]),
                                int(p["weight"]),
                                bool(p.get("is_active", True)),
                                int(p.get("sort_order", 0)),
                                now,
                            ),
                        )

                # seed default case and bind all existing prizes if cases are empty
                cur.execute("SELECT COUNT(*) FROM cases")
                cases_cnt = int(cur.fetchone()[0] or 0)
                if cases_cnt == 0:
                    now = int(time.time())
                    cur.execute(
                        "INSERT INTO cases (name, description, price, is_active, sort_order, created_at) "
                        "VALUES (%s,%s,%s,TRUE,0,%s) RETURNING id",
                        ("Стандарт", "Базовый кейс", 25, now),
                    )
                    default_case_id = int(cur.fetchone()[0])
                    # bind all prizes to default case with their current weights
                    cur.execute(
                        "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, created_at) "
                        "SELECT %s, id, GREATEST(weight,0), is_active, %s FROM prizes",
                        (default_case_id, now),
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
def _parse_init_data(init_data: str) -> dict:
    return dict(parse_qsl(init_data, keep_blank_values=True))


def _extract_user_json(init_data: str) -> Optional[str]:
    if not init_data:
        return None
    data = _parse_init_data(init_data)
    return data.get("user")


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


def extract_tg_user_public(init_data: str) -> Optional[dict]:
    """
    Extract public user fields from Telegram WebApp initData.user JSON.
    initData should already be validated by extract_tg_user_id() in the calling path.
    """
    user_json = _extract_user_json(init_data)
    if not user_json:
        return None
    try:
        user = json.loads(user_json)
        return {
            "username": user.get("username"),
            "first_name": user.get("first_name"),
            "last_name": user.get("last_name"),
            "photo_url": user.get("photo_url"),
        }
    except Exception:
        return None


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


# ===== Helpers =====
def mask_uid(uid: str) -> str:
    s = str(uid)
    tail = s[-4:] if len(s) >= 4 else s
    return f"User {tail}"


def display_name(username: Optional[str], first_name: Optional[str], last_name: Optional[str], uid: str) -> str:
    u = (username or "").strip()
    if u:
        return "@" + u.lstrip("@")
    full = ((first_name or "").strip() + " " + (last_name or "").strip()).strip()
    return full if full else mask_uid(uid)


def get_or_create_user(cur, tg_user_id: str, public: Optional[dict] = None) -> int:
    cur.execute(
        "INSERT INTO users (tg_user_id, balance, created_at) "
        "VALUES (%s, %s, %s) ON CONFLICT (tg_user_id) DO NOTHING",
        (tg_user_id, START_BALANCE, int(time.time())),
    )

    if public:
        cur.execute(
            "UPDATE users SET "
            "username = COALESCE(%s, username), "
            "first_name = COALESCE(%s, first_name), "
            "last_name = COALESCE(%s, last_name), "
            "photo_url = COALESCE(%s, photo_url) "
            "WHERE tg_user_id = %s",
            (
                public.get("username"),
                public.get("first_name"),
                public.get("last_name"),
                public.get("photo_url"),
                tg_user_id,
            ),
        )

    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (tg_user_id,))
    row = cur.fetchone()
    return int(row[0]) if row else START_BALANCE


def fetch_active_prizes(cur) -> list[dict]:
    cur.execute(
        "SELECT id, name, icon_url, cost, weight FROM prizes "
        "WHERE is_active = TRUE AND weight > 0 "
        "ORDER BY sort_order ASC, id ASC"
    )
    rows = cur.fetchall()
    return [{"id": int(r[0]), "name": str(r[1]), "icon_url": (str(r[2]).strip() if r[2] is not None else None), "cost": int(r[3]), "weight": int(r[4])} for r in rows]


def fetch_active_cases(cur) -> list[dict]:
    cur.execute(
        "SELECT id, name, description, price, is_active, sort_order FROM cases "
        "WHERE is_active = TRUE "
        "ORDER BY sort_order ASC, id ASC"
    )
    rows = cur.fetchall()
    return [{
        "id": int(r[0]),
        "name": str(r[1]),
        "description": (str(r[2]) if r[2] is not None else None),
        "price": int(r[3]),
        "is_active": bool(r[4]),
        "sort_order": int(r[5]),
    } for r in rows]


def fetch_case_prizes(cur, case_id: int) -> list[dict]:
    cur.execute(
        "SELECT p.id, p.name, p.icon_url, p.cost, cp.weight "
        "FROM case_prizes cp "
        "JOIN prizes p ON p.id = cp.prize_id "
        "WHERE cp.case_id=%s AND cp.is_active=TRUE AND p.is_active=TRUE AND cp.weight > 0 "
        "ORDER BY p.sort_order ASC, p.id ASC",
        (int(case_id),),
    )
    rows = cur.fetchall()
    return [{
        "id": int(r[0]),
        "name": str(r[1]),
        "icon_url": ((r[2] or '').strip() or None),
        "cost": int(r[3]),
        "weight": int(r[4]),
    } for r in rows]


def get_balance(cur, uid: str) -> int:
    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (uid,))
    row = cur.fetchone()
    return int(row[0]) if row else 0


# ===== Public API =====
@app.get("/")
def root():
    return {"ok": True}


@app.post("/me")
def me(req: MeReq):
    uid = extract_tg_user_id(req.initData)
    public = extract_tg_user_public(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                bal = get_or_create_user(cur, uid, public)
    return {"tg_user_id": uid, "balance": int(bal)}




@app.post("/prizes")
def prizes(req: MeReq):
    """
    Public list of active prizes for the frontend (roulette icons, prices).
    """
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                cur.execute(
                    "SELECT id, name, cost, icon_url "
                    "FROM prizes WHERE is_active = TRUE "
                    "ORDER BY sort_order ASC, id ASC"
                )
                rows = cur.fetchall()

    items = []
    for r in rows:
        icon_url = (r[3] or "").strip() or None
        items.append({"id": int(r[0]), "name": str(r[1]), "cost": int(r[2]), "icon_url": icon_url})
    return {"items": items}


@app.post("/cases")
def cases(req: MeReq):
    """Public list of active cases."""
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                items = fetch_active_cases(cur)
    return {"items": items}


@app.post("/cases/{case_id}/prizes")
def cases_prizes(case_id: int, req: MeReq):
    """Public list of prizes for a specific case."""
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                cur.execute("SELECT id, name, price FROM cases WHERE id=%s AND is_active=TRUE", (int(case_id),))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="case not found")
                items = fetch_case_prizes(cur, int(case_id))
    return {"case": {"id": int(row[0]), "name": str(row[1]), "price": int(row[2])}, "items": items}

@app.post("/inventory")
def inventory(req: InventoryReq):
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                cur.execute(
                    "SELECT i.id, i.prize_id, i.prize_name, i.prize_cost, i.created_at, "
                    "COALESCE(i.is_locked, FALSE) AS is_locked, i.locked_reason, "
                    "p.icon_url, COALESCE(p.is_unique, FALSE) AS is_unique "
                    "FROM inventory i "
                    "LEFT JOIN prizes p ON p.id = i.prize_id "
                    "WHERE i.tg_user_id=%s "
                    "ORDER BY i.created_at DESC LIMIT 200",
                    (uid,),
                )
                rows = cur.fetchall()

    return {"items": [{
        "inventory_id": int(r[0]),
        "prize_id": int(r[1]),
        "prize_name": str(r[2]),
        "prize_cost": int(r[3]),
        "created_at": int(r[4]),
        "is_locked": bool(r[5]),
        "locked_reason": (str(r[6]) if r[6] else None),
        "icon_url": ((r[7] or "").strip() or None),
        "is_unique": bool(r[8]),
    } for r in rows]}


@app.post("/inventory/sell")
def inventory_sell(req: InventorySellReq):
    uid = extract_tg_user_id(req.initData)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                cur.execute(
                    "SELECT id, prize_cost, COALESCE(is_locked,FALSE) "
                    "FROM inventory WHERE id=%s AND tg_user_id=%s FOR UPDATE",
                    (int(req.inventory_id), uid),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="inventory item not found")
                if bool(row[2]):
                    raise HTTPException(status_code=409, detail="item is locked")

                prize_cost = int(row[1])

                cur.execute("DELETE FROM inventory WHERE id=%s AND tg_user_id=%s", (int(req.inventory_id), uid))
                cur.execute(
                    "UPDATE users SET balance = balance + %s WHERE tg_user_id=%s RETURNING balance",
                    (prize_cost, uid),
                )
                new_balance = int(cur.fetchone()[0])

    return {"ok": True, "balance": new_balance, "credited": prize_cost}


@app.post("/inventory/withdraw")
def inventory_withdraw(req: InventoryWithdrawReq):
    uid = extract_tg_user_id(req.initData)

    # Step 1: lock inventory row and mark intent (commit before calling Telegram)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                cur.execute(
                    "SELECT id, prize_id, prize_name, prize_cost, COALESCE(is_locked,FALSE), locked_reason "
                    "FROM inventory WHERE id=%s AND tg_user_id=%s FOR UPDATE",
                    (int(req.inventory_id), uid),
                )
                inv = cur.fetchone()
                if not inv:
                    raise HTTPException(status_code=404, detail="inventory item not found")
                if bool(inv[4]):
                    return {"ok": True, "status": "locked", "reason": (inv[5] or None)}

                prize_id = int(inv[1])
                prize_name = str(inv[2])

                cur.execute("SELECT COALESCE(is_unique,FALSE), gift_id FROM prizes WHERE id=%s", (prize_id,))
                prow = cur.fetchone()
                is_unique = bool(prow[0]) if prow else False
                gift_id = (prow[1] if prow else None)

                now = int(time.time())
                if is_unique:
                    # Create admin claim and lock item
                    cur.execute(
                        "UPDATE inventory SET is_locked=TRUE, locked_reason=%s WHERE id=%s AND tg_user_id=%s",
                        ("claim_pending", int(req.inventory_id), uid),
                    )
                    cur.execute(
                        "INSERT INTO claims (tg_user_id, inventory_id, prize_id, prize_name, status, created_at) "
                        "VALUES (%s,%s,%s,%s,'pending',%s)",
                        (uid, int(req.inventory_id), prize_id, prize_name, now),
                    )
                    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (uid,))
                    bal = int(cur.fetchone()[0])
                    return {"ok": True, "status": "claim_created", "balance": bal}

                # Regular gift: lock as 'withdrawing'
                if not gift_id:
                    raise HTTPException(status_code=400, detail="gift_id is not configured for this prize")
                cur.execute(
                    "UPDATE inventory SET is_locked=TRUE, locked_reason=%s WHERE id=%s AND tg_user_id=%s",
                    ("withdrawing", int(req.inventory_id), uid),
                )

    # Step 2: call Telegram outside transaction
    try:
        tg_api("sendGift", {"user_id": int(uid), "gift_id": str(gift_id)})
    except HTTPException as e:
        # unlock on failure
        with pool.connection() as con2:
            with con2:
                with con2.cursor() as cur2:
                    cur2.execute(
                        "UPDATE inventory SET is_locked=FALSE, locked_reason=NULL "
                        "WHERE id=%s AND tg_user_id=%s AND locked_reason=%s",
                        (int(req.inventory_id), uid, "withdrawing"),
                    )
        raise e

    # Step 3: finalize (remove from inventory)
    with pool.connection() as con3:
        with con3:
            with con3.cursor() as cur3:
                cur3.execute(
                    "DELETE FROM inventory WHERE id=%s AND tg_user_id=%s AND locked_reason=%s",
                    (int(req.inventory_id), uid, "withdrawing"),
                )
                cur3.execute("SELECT balance FROM users WHERE tg_user_id=%s", (uid,))
                bal = int(cur3.fetchone()[0])
    return {"ok": True, "status": "sent", "balance": bal}


@app.post("/spin")
def spin(req: SpinReq):
    uid = extract_tg_user_id(req.initData)

    spin_id = str(uuid.uuid4())
    now = int(time.time())

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                # Determine case & price
                case_id = int(req.case_id) if req.case_id else 0
                case_name = None
                cost = None

                if case_id > 0:
                    cur.execute("SELECT id, name, price FROM cases WHERE id=%s AND is_active=TRUE", (case_id,))
                    crow = cur.fetchone()
                    if not crow:
                        raise HTTPException(status_code=404, detail="case not found")
                    case_id = int(crow[0])
                    case_name = str(crow[1])
                    cost = int(crow[2])
                else:
                    cur.execute(
                        "SELECT id, name, price FROM cases WHERE is_active=TRUE ORDER BY sort_order ASC, id ASC LIMIT 1"
                    )
                    crow = cur.fetchone()
                    if crow:
                        case_id = int(crow[0])
                        case_name = str(crow[1])
                        cost = int(crow[2])

                # Backward compatibility if no cases exist yet
                if cost is None:
                    cost = int(req.cost or 25)
                    if cost not in (25, 50):
                        raise HTTPException(status_code=400, detail="bad cost")

                # списываем ставку атомарно
                cur.execute(
                    "UPDATE users SET balance = balance - %s "
                    "WHERE tg_user_id=%s AND balance >= %s "
                    "RETURNING balance",
                    (cost, uid, cost),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=400, detail="balance too low")
                new_balance = int(row[0])

                # Prizes for selected case
                prizes = []
                if case_id > 0:
                    prizes = fetch_case_prizes(cur, case_id)
                if not prizes:
                    prizes = fetch_active_prizes(cur)
                if not prizes:
                    # fallback (если таблица пуста/всё отключено)
                    prizes = [{"id": p["id"], "name": p["name"], "icon_url": (p.get("icon_url") or None), "cost": p["cost"], "weight": p["weight"]} for p in DEFAULT_PRIZES]

                prize = random.choices(prizes, weights=[p["weight"] for p in prizes], k=1)[0]

                cur.execute(
                    "INSERT INTO spins (spin_id, tg_user_id, bet_cost, prize_id, prize_name, prize_cost, status, created_at, case_id, case_name, case_price) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s,%s)",
                    (
                        spin_id,
                        uid,
                        cost,
                        int(prize["id"]),
                        str(prize["name"]),
                        int(prize["cost"]),
                        now,
                        (case_id if case_id > 0 else None),
                        (case_name if case_name else None),
                        (cost if case_id > 0 else None),
                    ),
                )

    return {
        "spin_id": spin_id,
        "id": int(prize["id"]),
        "name": str(prize["name"]),
        "icon_url": ((prize.get("icon_url") or "").strip() or None),
        "cost": int(prize["cost"]),
        "balance": int(new_balance),
        "case_id": int(case_id) if case_id > 0 else None,
        "case_name": case_name,
        "bet_cost": int(cost),
    }


@app.post("/claim")
def claim(req: ClaimReq):
    uid = extract_tg_user_id(req.initData)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

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


@app.post("/leaderboard")
def leaderboard(req: LeaderboardReq):
    uid = extract_tg_user_id(req.initData)
    limit = max(5, min(100, int(req.limit or 30)))

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                my_balance = get_or_create_user(cur, uid, public)

                cur.execute(
                    "SELECT tg_user_id, balance, username, first_name, last_name, photo_url "
                    "FROM users ORDER BY balance DESC, created_at ASC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()

                cur.execute("SELECT 1 + COUNT(*) FROM users WHERE balance > %s", (my_balance,))
                my_rank = int(cur.fetchone()[0])

                cur.execute(
                    "SELECT username, first_name, last_name, photo_url FROM users WHERE tg_user_id=%s",
                    (uid,),
                )
                mine = cur.fetchone()

    items = []
    for i, r in enumerate(rows, start=1):
        tg_user_id = str(r[0])
        name = display_name(r[2], r[3], r[4], tg_user_id)
        avatar = (r[5] or "").strip() or None
        items.append({
            "rank": i,
            "tg_user_id": tg_user_id,
            "name": name,
            "avatar": avatar,
            "balance": int(r[1]),
            "is_me": tg_user_id == str(uid),
        })

    me_obj = {
        "rank": my_rank,
        "balance": int(my_balance),
        "name": display_name(mine[0], mine[1], mine[2], str(uid)) if mine else mask_uid(str(uid)),
        "avatar": ((mine[3] or "").strip() if mine else "") or None,
    }

    return {"items": items, "me": me_obj}


@app.post("/recent_wins")
def recent_wins(req: MeReq):
    """
    Recent spins with display name + avatar + prize.
    """
    uid = extract_tg_user_id(req.initData)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                cur.execute(
                    "SELECT s.tg_user_id, u.username, u.first_name, u.last_name, u.photo_url, s.prize_name, p.icon_url "
                    "FROM spins s "
                    "JOIN users u ON u.tg_user_id = s.tg_user_id LEFT JOIN prizes p ON p.id = s.prize_id "
                    "ORDER BY s.created_at DESC LIMIT 20"
                )
                rows = cur.fetchall()

    items = []
    for r in rows:
        tg_user_id = str(r[0])
        name = display_name(r[1], r[2], r[3], tg_user_id)
        avatar = (r[4] or "").strip() or None
        prize_name = str(r[5]) if r[5] is not None else ""
        items.append({"tg_user_id": tg_user_id, "name": name, "avatar": avatar, "prize": prize_name, "icon_url": ((r[6] or "").strip() or None)})

    return {"items": items}


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
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
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


# ===== Admin API =====
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

                cur.execute(
                    "SELECT COALESCE(SUM(stars_amount),0) FROM topups WHERE status='paid' AND paid_at >= %s",
                    (day_ago,),
                )
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
            "username": u[3],
            "first_name": u[4],
            "last_name": u[5],
            "photo_url": u[6],
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


# ===== Admin: CRUD prizes =====
@app.get("/admin/prizes")
def admin_list_prizes(request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT id, name, icon_url, cost, weight, gift_id, is_unique, is_active, sort_order, created_at "
                    "FROM prizes ORDER BY sort_order ASC, id ASC"
                )
                rows = cur.fetchall()
    items = []
    for r in rows:
        items.append({
            "id": int(r[0]),
            "name": str(r[1]),
            "icon_url": ((r[2] or "").strip() or None),
            "cost": int(r[3]),
            "weight": int(r[4]),
            "gift_id": (str(r[5]) if r[5] is not None and str(r[5]).strip() else None),
            "is_unique": bool(r[6]),
            "is_active": bool(r[7]),
            "sort_order": int(r[8]),
            "created_at": int(r[9]),
        })
    return {"items": items}


@app.post("/admin/prizes")
def admin_create_prize(request: Request, req: PrizeIn):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                # id вручную не принимаем, чтобы не ломать первичные ключи
                cur.execute("SELECT COALESCE(MAX(id),0) + 1 FROM prizes")
                new_id = int(cur.fetchone()[0])

                cur.execute(
                    "INSERT INTO prizes (id, name, icon_url, cost, weight, gift_id, is_unique, is_active, sort_order, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        new_id,
                        req.name,
                        (req.icon_url or None),
                        int(req.cost),
                        int(req.weight),
                        (req.gift_id or None),
                        bool(req.is_unique),
                        bool(req.is_active),
                        int(req.sort_order),
                        now,
                    ),
                )
    return {"id": new_id, "created_at": now, **req.model_dump()}


@app.put("/admin/prizes/{prize_id}")
def admin_update_prize(request: Request, prize_id: int, req: PrizeIn):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE prizes SET name=%s, icon_url=%s, cost=%s, weight=%s, gift_id=%s, is_unique=%s, "
                    "is_active=%s, sort_order=%s "
                    "WHERE id=%s RETURNING created_at",
                    (
                        req.name,
                        (req.icon_url or None),
                        int(req.cost),
                        int(req.weight),
                        (req.gift_id or None),
                        bool(req.is_unique),
                        bool(req.is_active),
                        int(req.sort_order),
                        int(prize_id),
                    ),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="prize not found")
                created_at = int(row[0])
    return {"id": int(prize_id), "created_at": created_at, **req.model_dump()}


@app.delete("/admin/prizes/{prize_id}")
def admin_delete_prize(request: Request, prize_id: int):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("DELETE FROM prizes WHERE id=%s RETURNING id", (int(prize_id),))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="prize not found")
    return {"ok": True, "deleted": int(prize_id)}


# ===== Admin: Cases =====
@app.get("/admin/cases")
def admin_list_cases(request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT id, name, description, price, is_active, sort_order, created_at "
                    "FROM cases ORDER BY sort_order ASC, id ASC"
                )
                rows = cur.fetchall()
    return {"items": [{
        "id": int(r[0]),
        "name": str(r[1]),
        "description": (str(r[2]) if r[2] is not None else None),
        "price": int(r[3]),
        "is_active": bool(r[4]),
        "sort_order": int(r[5]),
        "created_at": int(r[6]),
    } for r in rows]}


@app.post("/admin/cases")
def admin_create_case(request: Request, req: CaseIn):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "INSERT INTO cases (name, description, price, is_active, sort_order, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
                    (req.name, (req.description or None), int(req.price), bool(req.is_active), int(req.sort_order), now),
                )
                new_id = int(cur.fetchone()[0])
    return {"id": new_id, "created_at": now, **req.model_dump()}


@app.put("/admin/cases/{case_id}")
def admin_update_case(request: Request, case_id: int, req: CaseIn):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE cases SET name=%s, description=%s, price=%s, is_active=%s, sort_order=%s "
                    "WHERE id=%s RETURNING created_at",
                    (req.name, (req.description or None), int(req.price), bool(req.is_active), int(req.sort_order), int(case_id)),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="case not found")
                created_at = int(row[0])
    return {"id": int(case_id), "created_at": created_at, **req.model_dump()}


@app.delete("/admin/cases/{case_id}")
def admin_delete_case(request: Request, case_id: int):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("DELETE FROM cases WHERE id=%s RETURNING id", (int(case_id),))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="case not found")
    return {"ok": True, "deleted": int(case_id)}


@app.get("/admin/cases/{case_id}/prizes")
def admin_get_case_prizes(request: Request, case_id: int):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT prize_id, weight, is_active FROM case_prizes WHERE case_id=%s ORDER BY prize_id ASC",
                    (int(case_id),),
                )
                rows = cur.fetchall()
    return {"items": [{"prize_id": int(r[0]), "weight": int(r[1]), "is_active": bool(r[2])} for r in rows]}


@app.post("/admin/cases/{case_id}/prizes")
def admin_set_case_prizes(request: Request, case_id: int, items: list[CasePrizeIn]):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                # ensure case exists
                cur.execute("SELECT id FROM cases WHERE id=%s", (int(case_id),))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="case not found")

                cur.execute("DELETE FROM case_prizes WHERE case_id=%s", (int(case_id),))
                for it in items:
                    if int(it.weight) <= 0:
                        continue
                    cur.execute(
                        "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, created_at) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        (int(case_id), int(it.prize_id), int(it.weight), bool(it.is_active), now),
                    )
    return {"ok": True, "count": len(items)}


# ===== Admin: Claims =====
@app.get("/admin/claims")
def admin_list_claims(request: Request, status: str = Query("pending")):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT id, tg_user_id, inventory_id, prize_id, prize_name, status, created_at, processed_at "
                    "FROM claims WHERE status=%s ORDER BY created_at DESC LIMIT 500",
                    (status,),
                )
                rows = cur.fetchall()
    return {"items": [{
        "id": int(r[0]),
        "tg_user_id": str(r[1]),
        "inventory_id": int(r[2]),
        "prize_id": int(r[3]),
        "prize_name": str(r[4]),
        "status": str(r[5]),
        "created_at": int(r[6]),
        "processed_at": (int(r[7]) if r[7] is not None else None),
    } for r in rows]}


@app.post("/admin/claims/{claim_id}/approve")
def admin_approve_claim(request: Request, claim_id: int):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("UPDATE claims SET status='approved', processed_at=%s WHERE id=%s RETURNING inventory_id", (now, int(claim_id)))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="claim not found")
    return {"ok": True, "status": "approved", "claim_id": int(claim_id)}


@app.post("/admin/claims/{claim_id}/reject")
def admin_reject_claim(request: Request, claim_id: int):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT inventory_id FROM claims WHERE id=%s FOR UPDATE", (int(claim_id),))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="claim not found")
                inventory_id = int(row[0])

                cur.execute("UPDATE claims SET status='rejected', processed_at=%s WHERE id=%s", (now, int(claim_id)))
                cur.execute(
                    "UPDATE inventory SET is_locked=FALSE, locked_reason=NULL "
                    "WHERE id=%s AND locked_reason=%s",
                    (inventory_id, "claim_pending"),
                )
    return {"ok": True, "status": "rejected", "claim_id": int(claim_id)}


@app.post("/admin/claims/{claim_id}/fulfill")
def admin_fulfill_claim(request: Request, claim_id: int):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT inventory_id FROM claims WHERE id=%s FOR UPDATE", (int(claim_id),))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="claim not found")
                inventory_id = int(row[0])

                cur.execute("UPDATE claims SET status='fulfilled', processed_at=%s WHERE id=%s", (now, int(claim_id)))
                cur.execute("DELETE FROM inventory WHERE id=%s", (inventory_id,))
    return {"ok": True, "status": "fulfilled", "claim_id": int(claim_id)}