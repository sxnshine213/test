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

# Auto-configure webhook on startup (Render-friendly).
# Requires a public base URL (RENDER_EXTERNAL_URL or WEBHOOK_BASE_URL / WEBHOOK_URL).
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip()
AUTO_SET_WEBHOOK = os.getenv("AUTO_SET_WEBHOOK", "1").strip() not in ("0", "false", "False", "")

def _derive_webhook_url() -> str:
    if WEBHOOK_URL:
        return WEBHOOK_URL
    base = WEBHOOK_BASE_URL or RENDER_EXTERNAL_URL
    base = (base or "").rstrip("/")
    if not base:
        return ""
    return base + "/tg/webhook"

@app.on_event("startup")
def _startup_set_webhook():
    if not AUTO_SET_WEBHOOK:
        return
    if not BOT_TOKEN:
        return
    url = _derive_webhook_url()
    if not url:
        return
    payload = {
        "url": url,
        "allowed_updates": ["message", "callback_query", "pre_checkout_query"],
        "drop_pending_updates": False,
    }
    if TG_WEBHOOK_SECRET:
        payload["secret_token"] = TG_WEBHOOK_SECRET
    try:
        # setWebhook may occasionally fail transiently; do not prevent startup.
        tg_api_quick("setWebhook", payload, timeout=6.0, retries=2)
        print(f"[startup] webhook set to: {url}")
    except Exception as e:
        print(f"[startup] setWebhook failed: {e}")


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
    cost: int = 25
    case_id: Optional[int] = None


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

class AdminClaimNote(BaseModel):
    note: Optional[str] = None



class PrizeIn(BaseModel):
    name: str
    icon_url: Optional[str] = None
    cost: int
    weight: int
    is_active: bool = True
    sort_order: int = 0
    # Telegram Gift configuration:
    # - Regular gifts: set gift_id and keep is_unique=False (default)
    # - Unique gifts: set is_unique=True (gift_id can be empty); handled via admin claim flow
    gift_id: Optional[str] = None
    is_unique: bool = False


class CaseIn(BaseModel):
    name: str
    description: str = ""
    image_url: Optional[str] = None
    price: int = 25
    is_active: bool = True
    sort_order: int = 0


class CaseUpsert(BaseModel):
    id: int
    name: str
    description: str = ""
    image_url: Optional[str] = None
    price: int = 25
    is_active: bool = True
    sort_order: int = 0


class CaseOut(CaseIn):
    id: int
    created_at: int


class CasePrizeLinkIn(BaseModel):
    prize_id: int
    weight: int = 1
    is_active: bool = True
    sort_order: int = 0


class CasePrizeLinkOut(CasePrizeLinkIn):
    case_id: int





class PrizeOut(PrizeIn):
    id: int
    created_at: int


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

                cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_user_time ON spins(tg_user_id, created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_time ON spins(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_user_time ON inventory(tg_user_id, created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_topups_user_time ON topups(tg_user_id, created_at)")
                # gifts/withdrawals extensions
                cur.execute("ALTER TABLE prizes ADD COLUMN IF NOT EXISTS gift_id TEXT")
                cur.execute("ALTER TABLE prizes ADD COLUMN IF NOT EXISTS is_unique BOOLEAN DEFAULT FALSE")
                cur.execute("ALTER TABLE inventory ADD COLUMN IF NOT EXISTS is_locked BOOLEAN DEFAULT FALSE")
                cur.execute("ALTER TABLE inventory ADD COLUMN IF NOT EXISTS locked_reason TEXT")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS claims (
                      id BIGSERIAL PRIMARY KEY,
                      tg_user_id TEXT NOT NULL,
                      inventory_id BIGINT NOT NULL,
                      prize_id BIGINT NOT NULL,
                      prize_name TEXT NOT NULL,
                      prize_cost INTEGER NOT NULL,
                      status TEXT NOT NULL,
                      note TEXT,
                      created_at BIGINT NOT NULL,
                      updated_at BIGINT
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_claims_status_time ON claims(status, created_at)")


                # seed prizes if empty
                cur.execute("SELECT COUNT(*) FROM prizes")
                cnt = int(cur.fetchone()[0] or 0)
                if cnt == 0:
                    now = int(time.time())
                    for p in DEFAULT_PRIZES:
                        cur.execute(
                            "INSERT INTO prizes (id, name, icon_url, cost, weight, is_active, sort_order, created_at, gift_id, is_unique) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                int(p["id"]),
                                str(p["name"]),
                                (p.get("icon_url") or None),
                                int(p["cost"]),
                                int(p["weight"]),
                                bool(p.get("is_active", True)),
                                int(p.get("sort_order", 0)),
                                now,
                                None,
                                False,
                            ),
                        )

                # ===== Cases (multiple roulette cases with own prize pools) =====
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cases (
                      id BIGINT PRIMARY KEY,
                      name TEXT NOT NULL,
                      description TEXT,
                      image_url TEXT,
                      price INTEGER NOT NULL DEFAULT 25,
                      is_active BOOLEAN NOT NULL DEFAULT TRUE,
                      sort_order INTEGER NOT NULL DEFAULT 0,
                      created_at BIGINT NOT NULL
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cases_active_sort ON cases(is_active, sort_order, id)")
                cur.execute("ALTER TABLE cases ADD COLUMN IF NOT EXISTS description TEXT")
                cur.execute("ALTER TABLE cases ADD COLUMN IF NOT EXISTS image_url TEXT")
                cur.execute("ALTER TABLE cases ADD COLUMN IF NOT EXISTS price INTEGER NOT NULL DEFAULT 25")
                cur.execute("ALTER TABLE cases ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE")
                cur.execute("ALTER TABLE cases ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE cases ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS case_prizes (
                      case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
                      prize_id BIGINT NOT NULL REFERENCES prizes(id) ON DELETE CASCADE,
                      weight INTEGER NOT NULL DEFAULT 1,
                      is_active BOOLEAN NOT NULL DEFAULT TRUE,
                      sort_order INTEGER NOT NULL DEFAULT 0,
                      created_at BIGINT NOT NULL,
                      PRIMARY KEY (case_id, prize_id)
                    )
                    """
                )
                # Older DBs might already have case_prizes without these columns:
                cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS weight INTEGER NOT NULL DEFAULT 1")
                cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE")
                cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_case_prizes_case_active_sort "
                    "ON case_prizes(case_id, is_active, sort_order, prize_id)"
                )

                # Store chosen case in spins (optional, but helps analytics/admin)
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_id BIGINT")
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_name TEXT")

                # Seed default cases + mappings (only if empty)
                cur.execute("SELECT COUNT(*) FROM cases")
                case_cnt = int(cur.fetchone()[0] or 0)
                if case_cnt == 0:
                    now2 = int(time.time())
                    defaults = [
                        {"id": 1, "name": "🥉 Bronze Case", "description": "Базовый кейс", "image_url": "", "price": 25, "is_active": True, "sort_order": 10},
                        {"id": 2, "name": "🥈 Silver Case", "description": "Шансы чуть лучше", "image_url": "", "price": 50, "is_active": True, "sort_order": 20},
                        {"id": 3, "name": "🥇 Gold Case", "description": "Топовый кейс", "image_url": "", "price": 75, "is_active": True, "sort_order": 30},
                    ]
                    for c in defaults:
                        cur.execute(
                            "INSERT INTO cases (id, name, description, image_url, price, is_active, sort_order, created_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                            (
                                int(c["id"]),
                                str(c["name"]),
                                str(c.get("description") or ""),
                                (c.get("image_url") or None),
                                int(c.get("price") or 25),
                                bool(c.get("is_active", True)),
                                int(c.get("sort_order", 0)),
                                now2,
                            ),
                        )

                    # Create default mapping for each case from active prizes
                    cur.execute("SELECT id, cost, weight, sort_order, is_active FROM prizes")
                    all_pr = cur.fetchall()
                    for c in defaults:
                        cid = int(c["id"])
                        for (pid, pcost, pweight, psort, pact) in all_pr:
                            if pact is False:
                                continue
                            base_w = int(pweight or 1)
                            # bias expensive cases slightly towards expensive prizes
                            mult = 1
                            if int(c.get("price") or 25) >= 50:
                                mult += int((int(pcost or 0)) // 50)
                            if int(c.get("price") or 25) >= 75:
                                mult += int((int(pcost or 0)) // 75)
                            w = max(1, base_w * mult)
                            cur.execute(
                                "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, sort_order, created_at) "
                                "VALUES (%s,%s,%s,TRUE,%s,%s) "
                                "ON CONFLICT (case_id, prize_id) DO NOTHING",
                                (cid, int(pid), int(w), int(psort or 0), now2),
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
    """Call Telegram Bot API.
    - On HTTP/network errors -> HTTP 502
    - On Telegram 'ok=false' -> propagate Telegram error_code (e.g. 400/403) and description
    """
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not set")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

    raw = None
    obj = None
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
    except Exception as e:
        # urllib may raise HTTPError; it still contains body
        try:
            if hasattr(e, "read"):
                raw = e.read().decode("utf-8")
        except Exception:
            raw = None
        if raw:
            try:
                obj = json.loads(raw)
            except Exception:
                obj = None
        # If Telegram responded with JSON error, surface it below.
        if obj and isinstance(obj, dict) and obj.get("ok") is False:
            code = int(obj.get("error_code") or 502)
            desc = obj.get("description") or str(obj)
            raise HTTPException(status_code=code, detail=f"telegram: {desc}")
        raise HTTPException(status_code=502, detail=f"telegram api error: {e}")

    try:
        obj = json.loads(raw or "{}")
    except Exception:
        raise HTTPException(status_code=502, detail=f"telegram api invalid json: {raw!r}")

    if not obj.get("ok"):
        code = int(obj.get("error_code") or 502)
        desc = obj.get("description") or str(obj)
        raise HTTPException(status_code=code, detail=f"telegram: {desc}")
    return obj.get("result")



def tg_api_timeout(method: str, payload: dict, timeout: float):
    """Telegram Bot API call with a custom timeout (seconds)."""
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not set")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    raw = None
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"telegram api error: {e}")

    try:
        obj = json.loads(raw or "{}")
    except Exception:
        raise HTTPException(status_code=502, detail=f"telegram api invalid json: {raw!r}")

    if not obj.get("ok"):
        code = int(obj.get("error_code") or 502)
        desc = obj.get("description") or str(obj)
        raise HTTPException(status_code=code, detail=f"telegram: {desc}")
    return obj.get("result")


def tg_api_quick(method: str, payload: dict, timeout: float = 4.0, retries: int = 2):
    """Fast Telegram call for time-sensitive flows (e.g., answerPreCheckoutQuery)."""
    last = None
    for i in range(max(1, int(retries))):
        try:
            return tg_api_timeout(method, payload, timeout=timeout)
        except Exception as e:
            last = e
            if i < retries - 1:
                time.sleep(0.2)
    if last:
        raise last
    raise HTTPException(status_code=502, detail="telegram api error")

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



def get_balance(cur, tg_user_id: str) -> int:
    """Return user's current balance from DB. Assumes user exists."""
    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (tg_user_id,))
    row = cur.fetchone()
    # Should exist because get_or_create_user() is called before most endpoints
    return int(row[0]) if row and row[0] is not None else 0


def fetch_active_prizes(cur) -> list[dict]:
    cur.execute(
        "SELECT id, name, icon_url, cost, weight FROM prizes "
        "WHERE is_active = TRUE AND weight > 0 "
        "ORDER BY sort_order ASC, id ASC"
    )
    rows = cur.fetchall()
    return [{"id": int(r[0]), "name": str(r[1]), "icon_url": (str(r[2]).strip() if r[2] is not None else None), "cost": int(r[3]), "weight": int(r[4])} for r in rows]



def fetch_active_cases(cur, include_inactive: bool = False) -> list[dict]:
    if include_inactive:
        cur.execute(
            "SELECT id, name, description, image_url, price, is_active, sort_order "
            "FROM cases ORDER BY sort_order ASC, id ASC"
        )
    else:
        cur.execute(
            "SELECT id, name, description, image_url, price, is_active, sort_order "
            "FROM cases WHERE is_active = TRUE ORDER BY sort_order ASC, id ASC"
        )
    rows = cur.fetchall()
    return [
        {
            "id": int(r[0]),
            "name": str(r[1]),
            "description": (str(r[2]) if r[2] is not None else ""),
            "image_url": (str(r[3]) if r[3] is not None else ""),
            "price": int(r[4]),
            "is_active": bool(r[5]),
            "sort_order": int(r[6]),
        }
        for r in rows
    ]


def fetch_case_prizes(cur, case_id: int, include_inactive: bool = False) -> list[dict]:
    q = (
        "SELECT p.id, p.name, p.icon_url, p.cost, cp.weight, p.gift_id, p.is_unique, cp.is_active, cp.sort_order "
        "FROM case_prizes cp "
        "JOIN prizes p ON p.id = cp.prize_id "
        "WHERE cp.case_id = %s "
    )
    args = [int(case_id)]
    if not include_inactive:
        q += "AND cp.is_active = TRUE AND p.is_active = TRUE AND cp.weight > 0 "
    q += "ORDER BY cp.sort_order ASC, p.sort_order ASC, p.id ASC"
    cur.execute(q, args)
    rows = cur.fetchall()
    return [
        {
            "id": int(r[0]),
            "name": str(r[1]),
            "icon_url": (str(r[2]) if r[2] is not None else None),
            "cost": int(r[3]),
            "weight": int(r[4]),
            "gift_id": (str(r[5]) if r[5] is not None else None),
            "is_unique": bool(r[6]) if r[6] is not None else False,
            "link_is_active": bool(r[7]) if r[7] is not None else True,
            "link_sort_order": int(r[8]) if r[8] is not None else 0,
        }
        for r in rows
    ]




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





@app.get("/cases")
def list_cases():
    """Public list of available cases for the frontend."""
    with pool.connection() as con:
        with con.cursor() as cur:
            items = fetch_active_cases(cur, include_inactive=False)
    return {"items": items}


@app.get("/cases/{case_id}/prizes")
def list_case_prizes(case_id: int):
    """Public list of prizes for a given case (roulette contents)."""
    with pool.connection() as con:
        with con.cursor() as cur:
            # ensure case exists & active
            cur.execute("SELECT id, name, description, image_url, price, is_active, sort_order FROM cases WHERE id=%s", (int(case_id),))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="case not found")
            if not bool(row[5]):
                raise HTTPException(status_code=404, detail="case not active")

            items = fetch_case_prizes(cur, int(case_id), include_inactive=False)
            if not items:
                # fallback to global active prizes if mapping missing
                items = fetch_active_prizes(cur)

    case_obj = {
        "id": int(row[0]),
        "name": str(row[1]),
        "description": (str(row[2]) if row[2] is not None else ""),
        "image_url": (str(row[3]) if row[3] is not None else ""),
        "price": int(row[4]),
        "is_active": bool(row[5]),
        "sort_order": int(row[6]),
    }
    return {"case": case_obj, "items": items}


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

                # NOTE: order here is important to avoid mapping bugs
                cur.execute(
                    "SELECT id, name, cost, icon_url "
                    "FROM prizes WHERE is_active = TRUE AND weight > 0 "
                    "ORDER BY sort_order ASC, id ASC"
                )
                rows = cur.fetchall()

    items = []
    for r in rows:
        # r = (id, name, cost, icon_url)
        items.append(
            {
                "id": int(r[0]),
                "name": str(r[1]),
                "cost": int(r[2]),
                "icon_url": (str(r[3]).strip() if r[3] is not None and str(r[3]).strip() else None),
            }
        )
    return {"items": items}

@app.post("/inventory")
def inventory(req: InventoryReq):
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                cur.execute(
                    "SELECT i.id, i.prize_id, i.prize_name, i.prize_cost, i.created_at, p.icon_url "
                    "FROM inventory i LEFT JOIN prizes p ON p.id = i.prize_id WHERE i.tg_user_id=%s "
                    "ORDER BY created_at DESC LIMIT 200",
                    (uid,),
                )
                rows = cur.fetchall()

    return {"items": [{
        "inventory_id": int(r[0]),
        "prize_id": int(r[1]),
        "prize_name": r[2],
        "prize_cost": int(r[3]),
        "created_at": int(r[4]),
        "icon_url": ((r[5] or "").strip() or None),
    } for r in rows]}


@app.post("/inventory/sell")
def inventory_sell(req: InventorySellReq):
    uid = extract_tg_user_id(req.initData)
    inv_id = int(req.inventory_id)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                cur.execute(
                    "SELECT prize_cost FROM inventory WHERE id=%s AND tg_user_id=%s FOR UPDATE",
                    (inv_id, uid),
                )
                row = cur.fetchone()
                if row and len(row) >= 4 and bool(row[3]):
                    raise HTTPException(status_code=409, detail="item is locked")
                if not row:
                    raise HTTPException(status_code=404, detail="inventory item not found")

                prize_cost = int(row[0] or 0)

                cur.execute("DELETE FROM inventory WHERE id=%s AND tg_user_id=%s", (inv_id, uid))
                cur.execute(
                    "UPDATE users SET balance=balance+%s WHERE tg_user_id=%s RETURNING balance",
                    (prize_cost, uid),
                )
                bal = int(cur.fetchone()[0])

    return {"ok": True, "inventory_id": inv_id, "credited": prize_cost, "balance": bal}



@app.post("/inventory/withdraw")
def inventory_withdraw(req: InventoryWithdrawReq):
    """
    Withdraw inventory item:
      - Regular prize (is_unique = FALSE): bot sends gift via sendGift and item is removed from inventory
      - Unique prize (is_unique = TRUE): create a claim for admins and lock the inventory item
    """
    uid = extract_tg_user_id(req.initData)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                # Lock inventory row to avoid double-withdraw/sell.
                # IMPORTANT: do not use LEFT JOIN ... FOR UPDATE (Postgres forbids locking the nullable side of an outer join).
                # We lock the inventory row first, then read prize properties in a separate query.
                cur.execute(
                    "SELECT id, prize_id, prize_name, prize_cost, COALESCE(is_locked, FALSE) AS is_locked "
                    "FROM inventory "
                    "WHERE id = %s AND tg_user_id = %s "
                    "FOR UPDATE",
                    (int(req.inventory_id), uid),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="inventory item not found")

                inv_id, prize_id, prize_name, prize_cost, is_locked = row

                # Read prize attributes (is_unique, gift_id). Prize row might be missing if admin deleted it; handle safely.
                cur.execute(
                    "SELECT COALESCE(is_unique, FALSE) AS is_unique, COALESCE(gift_id, '') AS gift_id "
                    "FROM prizes WHERE id = %s",
                    (int(prize_id),),
                )
                prow = cur.fetchone()
                if prow:
                    is_unique, gift_id = bool(prow[0]), str(prow[1] or "")
                else:
                    is_unique, gift_id = False, ""

                if is_locked:
                    # idempotent response for already requested unique gifts
                    cur.execute(
                        "SELECT id, status FROM claims WHERE inventory_id = %s ORDER BY created_at DESC LIMIT 1",
                        (int(inv_id),),
                    )
                    c = cur.fetchone()
                    if c:
                        return {"ok": True, "status": str(c[1]), "message": "Заявка уже существует."}
                    raise HTTPException(status_code=409, detail="item is locked")

                if bool(is_unique):
                    now = int(time.time())
                    # create claim
                    cur.execute(
                        "INSERT INTO claims (tg_user_id, inventory_id, prize_id, prize_name, prize_cost, status, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,'pending',%s) RETURNING id",
                        (uid, int(inv_id), int(prize_id), str(prize_name), int(prize_cost), now),
                    )
                    claim_id = int(cur.fetchone()[0])
                    # lock item in inventory until admins process
                    cur.execute(
                        "UPDATE inventory SET is_locked = TRUE, locked_reason = 'claim_pending' WHERE id = %s",
                        (int(inv_id),),
                    )
                    return {"ok": True, "status": "pending", "claim_id": claim_id, "message": "Заявка на уникальный подарок создана."}

                # regular gifts: send by bot
                gid = (gift_id or "").strip()
                if not gid:
                    raise HTTPException(status_code=400, detail="gift_id is not configured for this prize")

                # Bot API: sendGift supports user_id or chat_id. Use user_id for private users.
                tg_api("sendGift", {"gift_id": gid, "user_id": int(uid)})

                # remove from inventory
                cur.execute("DELETE FROM inventory WHERE id = %s AND tg_user_id = %s", (int(inv_id), uid))

                bal = get_balance(cur, uid)
                return {"ok": True, "status": "sent", "message": "Подарок отправлен ботом.", "balance": int(bal)}



@app.post("/spin")
def spin(req: SpinReq):
    uid = extract_tg_user_id(req.initData)
    case_id = int(req.case_id) if req.case_id is not None else None

    # legacy режим (если кейсы не используются)
    cost = int(req.cost or 25)
    if case_id is None and cost not in (25, 50):
        raise HTTPException(status_code=400, detail="bad cost")

    spin_id = str(uuid.uuid4())
    now = int(time.time())

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                case_name = None
                if case_id is not None:
                    cur.execute(
                        "SELECT id, name, price, is_active FROM cases WHERE id=%s",
                        (int(case_id),),
                    )
                    c = cur.fetchone()
                    if not c:
                        raise HTTPException(status_code=404, detail="case not found")
                    if not bool(c[3]):
                        raise HTTPException(status_code=400, detail="case not active")
                    cost = int(c[2])
                    case_name = str(c[1])

                # списываем ставку атомарно
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

                # выбираем призы (по кейсу или глобально)
                prizes = []
                if case_id is not None:
                    try:
                        prizes = fetch_case_prizes(cur, int(case_id), include_inactive=False)
                    except Exception:
                        prizes = []
                if not prizes:
                    prizes = fetch_active_prizes(cur)

                if not prizes:
                    # fallback (если таблица пуста/всё отключено)
                    prizes = [
                        {
                            "id": int(p["id"]),
                            "name": p["name"],
                            "icon_url": p.get("icon_url"),
                            "cost": int(p["cost"]),
                            "weight": int(p["weight"]),
                        }
                        for p in DEFAULT_PRIZES
                    ]

                prize = random.choices(prizes, weights=[int(p.get("weight") or 1) for p in prizes], k=1)[0]

                cur.execute(
                    "INSERT INTO spins (spin_id, tg_user_id, bet_cost, case_id, case_name, prize_id, prize_name, prize_cost, status, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s)",
                    (
                        spin_id,
                        uid,
                        cost,
                        (int(case_id) if case_id is not None else None),
                        case_name,
                        int(prize["id"]),
                        str(prize["name"]),
                        int(prize["cost"]),
                        now,
                    ),
                )

    return {
        "spin_id": spin_id,
        "case_id": (int(case_id) if case_id is not None else None),
        "case_name": case_name,
        "id": int(prize["id"]),
        "name": str(prize["name"]),
        "icon_url": (str(prize.get("icon_url")) if prize.get("icon_url") else None),
        "cost": int(prize["cost"]),
        "balance": int(new_balance),
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
        "provider_token": "",
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
        # Validate Stars invoice before approving. This prevents accidental/double payments
        # and protects from mismatched payload/amount.
        try:
            currency = q.get("currency")
            total_amount = int(q.get("total_amount", 0))
            invoice_payload = q.get("invoice_payload", "")
            from_id = str((q.get("from") or {}).get("id") or "")

            ok = True
            err = None

            if currency != "XTR":
                ok = False
                err = "Unsupported currency"
            elif total_amount <= 0:
                ok = False
                err = "Bad amount"
            elif not invoice_payload:
                ok = False
                err = "Missing payload"

            if ok:
                with pool.connection() as con:
                    with con:
                        with con.cursor() as cur:
                            cur.execute("SET LOCAL statement_timeout = %s", ("2500ms",))
                            cur.execute(
                                "SELECT tg_user_id, stars_amount, status FROM topups WHERE payload=%s FOR UPDATE",
                                (invoice_payload,),
                            )
                            row = cur.fetchone()
                            if not row:
                                ok = False
                                err = "Unknown invoice"
                            else:
                                uid, expected, status = str(row[0]), int(row[1]), str(row[2])
                                if status == "paid":
                                    # already processed; allow Telegram to proceed, we'll no-op on successful_payment
                                    ok = True
                                elif uid != from_id:
                                    ok = False
                                    err = "Wrong payer"
                                elif expected != total_amount:
                                    ok = False
                                    err = "Amount mismatch"
                                elif status not in ("created", "pending"):
                                    # any other status means we don't expect a payment right now
                                    ok = False
                                    err = "Bad status"

            payload = {"pre_checkout_query_id": q["id"], "ok": bool(ok)}
            if not ok:
                payload["error_message"] = err or "Payment rejected"
            tg_api_quick("answerPreCheckoutQuery", payload, timeout=4.0, retries=2)
        except Exception:
            # In case of unexpected errors (DB cold start / transient network), try to approve to avoid hanging the payment UI.
            # If something is wrong, we will reconcile later via getStarTransactions.
            try:
                tg_api_quick("answerPreCheckoutQuery", {"pre_checkout_query_id": q.get("id"), "ok": True}, timeout=4.0, retries=2)
            except Exception:
                pass
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

class WebhookSetupReq(BaseModel):
    url: Optional[str] = None


@app.get("/admin/webhook_info")
def admin_webhook_info(request: Request):
    require_admin(request)
    return {"result": tg_api("getWebhookInfo", {})}


@app.post("/admin/setup_webhook")
def admin_setup_webhook(request: Request, req: WebhookSetupReq):
    require_admin(request)
    url = (req.url or "").strip() or _derive_webhook_url()
    if not url:
        raise HTTPException(status_code=400, detail="No webhook url (set WEBHOOK_URL or RENDER_EXTERNAL_URL)")
    payload = {
        "url": url,
        "allowed_updates": ["message", "callback_query", "pre_checkout_query"],
        "drop_pending_updates": False,
    }
    if TG_WEBHOOK_SECRET:
        payload["secret_token"] = TG_WEBHOOK_SECRET
    tg_api("setWebhook", payload)
    return {"ok": True, "url": url}

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




@app.get("/admin/my_star_balance")
def admin_my_star_balance(request: Request):
    """Return bot's current Telegram Stars balance (Bot API 9.1+)."""
    require_admin(request)
    # getMyStarBalance returns a StarAmount object in Bot API. Surface raw result.
    result = tg_api("getMyStarBalance", {})
    return {"ok": True, "result": result}

@app.get("/admin/star_transactions")
def admin_star_transactions(request: Request, limit: int = 50, offset: int | None = None):
    """Debug helper: fetch recent Telegram Stars transactions for the bot."""
    require_admin(request)
    payload: dict = {"limit": int(limit)}
    if offset is not None:
        payload["offset"] = int(offset)
    result = tg_api("getStarTransactions", payload)
    return {"ok": True, "result": result}


def _find_invoice_payload(obj):
    """Best-effort recursive search for invoice_payload in StarTransaction structure."""
    if isinstance(obj, dict):
        if "invoice_payload" in obj and obj["invoice_payload"]:
            return obj["invoice_payload"]
        for v in obj.values():
            found = _find_invoice_payload(v)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_invoice_payload(v)
            if found:
                return found
    return None


def _find_star_amount(obj):
    """Best-effort: return integer Stars amount from StarTransaction structure."""
    if isinstance(obj, dict):
        # common keys: amount, star_amount, total_amount
        for k in ("amount", "star_amount", "total_amount"):
            if k in obj and obj[k] is not None:
                try:
                    return int(obj[k])
                except Exception:
                    pass
        for v in obj.values():
            found = _find_star_amount(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_star_amount(v)
            if found is not None:
                return found
    return None


@app.post("/admin/reconcile_star_transactions")
def admin_reconcile_star_transactions(request: Request, limit: int = 200):
    """
    Safety net: reconcile bot Stars transactions with local `topups` table.
    Use if webhook was down and some successful payments were missed.
    """
    require_admin(request)
    result = tg_api("getStarTransactions", {"limit": int(limit)})
    txs = (result or {}).get("transactions") or (result or {}).get("result", {}).get("transactions") or []
    fixed = 0
    checked = 0

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                for tx in txs:
                    checked += 1
                    payload = _find_invoice_payload(tx)
                    if not payload:
                        continue
                    amt = _find_star_amount(tx)
                    if amt is None:
                        continue
                    tx_id = str(tx.get("id") or "")

                    cur.execute("SELECT tg_user_id, stars_amount, status FROM topups WHERE payload=%s FOR UPDATE", (payload,))
                    row = cur.fetchone()
                    if not row:
                        continue
                    uid, expected, status = str(row[0]), int(row[1]), str(row[2])
                    if status == "paid":
                        continue
                    if expected != int(amt):
                        continue

                    # Apply the same accounting as in webhook
                    cur.execute("UPDATE users SET balance = balance + %s WHERE tg_user_id=%s", (expected, uid))
                    cur.execute(
                        "UPDATE topups SET status='paid', telegram_charge_id=%s, paid_at=%s WHERE payload=%s",
                        (tx_id or None, int(time.time()), payload),
                    )
                    fixed += 1

    return {"ok": True, "checked": checked, "fixed": fixed}


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
                    "SELECT id, name, icon_url, cost, weight, is_active, sort_order, created_at, COALESCE(gift_id,''), COALESCE(is_unique,FALSE) "
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
            "is_active": bool(r[5]),
            "sort_order": int(r[6]),
            "created_at": int(r[7]), "gift_id": (r[8] or "").strip() or None, "is_unique": bool(r[9]),
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
                    "INSERT INTO prizes (id, name, icon_url, cost, weight, is_active, sort_order, created_at, gift_id, is_unique) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (new_id, req.name, (req.icon_url or None), int(req.cost), int(req.weight), bool(req.is_active), int(req.sort_order), now, (req.gift_id or None), bool(req.is_unique)),
                )
    return {"id": new_id, "created_at": now, **req.model_dump()}


@app.put("/admin/prizes/{prize_id}")
def admin_update_prize(request: Request, prize_id: int, req: PrizeIn):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE prizes SET name=%s, icon_url=%s, cost=%s, weight=%s, is_active=%s, sort_order=%s, gift_id=%s, is_unique=%s "
                    "WHERE id=%s RETURNING created_at",
                    (req.name, (req.icon_url or None), int(req.cost), int(req.weight), bool(req.is_active), int(req.sort_order), (req.gift_id or None), bool(req.is_unique), int(prize_id)),
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




# ===== Admin: CRUD cases =====
@app.get("/admin/cases")
def admin_list_cases(request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con.cursor() as cur:
            items = fetch_active_cases(cur, include_inactive=True)
    return {"ok": True, "items": items}


@app.post("/admin/cases/bulk")
def admin_cases_bulk(request: Request, items: list[CaseUpsert]):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                for c in items:
                    cur.execute(
                        "INSERT INTO cases (id, name, description, image_url, price, is_active, sort_order, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "name=EXCLUDED.name, description=EXCLUDED.description, image_url=EXCLUDED.image_url, "
                        "price=EXCLUDED.price, is_active=EXCLUDED.is_active, sort_order=EXCLUDED.sort_order",
                        (
                            int(c.id),
                            str(c.name),
                            str(c.description or ""),
                            (c.image_url or None),
                            int(c.price or 25),
                            bool(c.is_active),
                            int(c.sort_order or 0),
                            now,
                        ),
                    )
    return {"ok": True, "count": len(items)}


@app.delete("/admin/cases/{case_id}")
def admin_delete_case(case_id: int, request: Request):
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
def admin_get_case_prizes(case_id: int, request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con.cursor() as cur:
            cur.execute("SELECT id FROM cases WHERE id=%s", (int(case_id),))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="case not found")

            # all prizes + mapping for the case
            cur.execute(
                "SELECT id, name, icon_url, cost, weight, is_active, sort_order, gift_id, is_unique "
                "FROM prizes ORDER BY sort_order ASC, id ASC"
            )
            prizes = [
                {
                    "id": int(r[0]),
                    "name": str(r[1]),
                    "icon_url": (str(r[2]) if r[2] is not None else None),
                    "cost": int(r[3]),
                    "global_weight": int(r[4]),
                    "global_is_active": bool(r[5]),
                    "global_sort_order": int(r[6]),
                    "gift_id": (str(r[7]) if r[7] is not None else None),
                    "is_unique": bool(r[8]) if r[8] is not None else False,
                }
                for r in cur.fetchall()
            ]

            cur.execute(
                "SELECT prize_id, weight, is_active, sort_order "
                "FROM case_prizes WHERE case_id=%s ORDER BY sort_order ASC, prize_id ASC",
                (int(case_id),),
            )
            mapping = [
                {"prize_id": int(r[0]), "weight": int(r[1]), "is_active": bool(r[2]), "sort_order": int(r[3])}
                for r in cur.fetchall()
            ]

    return {"ok": True, "case_id": int(case_id), "prizes": prizes, "mapping": mapping}


@app.post("/admin/cases/{case_id}/prizes/bulk")
def admin_set_case_prizes(case_id: int, request: Request, items: list[CasePrizeLinkIn]):
    require_admin(request)
    now = int(time.time())
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT id FROM cases WHERE id=%s", (int(case_id),))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="case not found")

                cur.execute("DELETE FROM case_prizes WHERE case_id=%s", (int(case_id),))
                for it in items:
                    cur.execute(
                        "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, sort_order, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            int(case_id),
                            int(it.prize_id),
                            max(0, int(it.weight or 0)),
                            bool(it.is_active),
                            int(it.sort_order or 0),
                            now,
                        ),
                    )

    return {"ok": True, "case_id": int(case_id), "count": len(items)}


@app.get("/admin/claims")
def admin_claims(request: Request, status: str = Query("pending"), limit: int = Query(100, ge=1, le=500)):
    require_admin(request)
    with pool.connection() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT id, tg_user_id, inventory_id, prize_id, prize_name, prize_cost, status, note, created_at, updated_at "
                "FROM claims "
                "WHERE (%s = '' OR status = %s) "
                "ORDER BY created_at DESC "
                "LIMIT %s",
                (status or "", status or "", int(limit)),
            )
            rows = cur.fetchall()

    items = []
    for r in rows:
        items.append(
            {
                "id": int(r[0]),
                "tg_user_id": r[1],
                "inventory_id": int(r[2]),
                "prize_id": int(r[3]),
                "prize_name": r[4],
                "prize_cost": int(r[5]),
                "status": r[6],
                "note": r[7],
                "created_at": int(r[8]),
                "updated_at": int(r[9] or 0) or None,
            }
        )
    return {"items": items}


def _set_claim_status(cur, claim_id: int, new_status: str, note: Optional[str] = None):
    now = int(time.time())
    cur.execute("SELECT id, inventory_id, status FROM claims WHERE id = %s", (int(claim_id),))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="claim not found")
    inv_id = int(row[1])
    cur.execute(
        "UPDATE claims SET status = %s, note = COALESCE(%s, note), updated_at = %s WHERE id = %s",
        (new_status, note, now, int(claim_id)),
    )
    return inv_id


@app.post("/admin/claims/{claim_id}/approve")
def admin_claim_approve(claim_id: int, req: AdminClaimNote, request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                _set_claim_status(cur, claim_id, "approved", req.note)
    return {"ok": True, "id": int(claim_id), "status": "approved"}


@app.post("/admin/claims/{claim_id}/reject")
def admin_claim_reject(claim_id: int, req: AdminClaimNote, request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                inv_id = _set_claim_status(cur, claim_id, "rejected", req.note)
                # unlock inventory so user can sell/withdraw again
                cur.execute("UPDATE inventory SET is_locked = FALSE, locked_reason = NULL WHERE id = %s", (int(inv_id),))
    return {"ok": True, "id": int(claim_id), "status": "rejected"}


@app.post("/admin/claims/{claim_id}/fulfill")
def admin_claim_fulfill(claim_id: int, req: AdminClaimNote, request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                inv_id = _set_claim_status(cur, claim_id, "fulfilled", req.note)
                # remove item from inventory - it has been handed over manually
                cur.execute("DELETE FROM inventory WHERE id = %s", (int(inv_id),))
    return {"ok": True, "id": int(claim_id), "status": "fulfilled"}




if __name__ == '__main__':
    import os
    import uvicorn
    port = int(os.environ.get('PORT', '8000'))
    uvicorn.run('server:app', host='0.0.0.0', port=port, proxy_headers=True)
