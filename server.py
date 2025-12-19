import os
import json
import time
import random
import uuid
import hmac
import hashlib
import secrets
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

SPIN_COOLDOWN_SEC = float(os.environ.get("SPIN_COOLDOWN_SEC", "2.0"))
REQUIRE_CLAIM_BEFORE_NEXT_SPIN = os.environ.get("REQUIRE_CLAIM_BEFORE_NEXT_SPIN", "1").strip() not in ("0", "false", "False", "")
IDEMPOTENCY_TTL_SEC = int(os.environ.get("IDEMPOTENCY_TTL_SEC", str(24 * 3600)))
MAX_CLIENT_SEED_LEN = int(os.environ.get("MAX_CLIENT_SEED_LEN", "64"))


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


class CasesReq(WithInitData):
    pass


class PrizesReq(WithInitData):
    case_id: Optional[int] = None



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

class FairnessStateReq(WithInitData):
    pass

class FairnessSetClientSeedReq(WithInitData):
    client_seed: str


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



class PrizeOut(PrizeIn):
    id: int
    created_at: int

class CaseIn(BaseModel):
    name: str
    image_url: Optional[str] = None
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
    sort_order: int = 0


class CasePrizesReplaceReq(BaseModel):
    items: list[CasePrizeIn]



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


                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS banned BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS client_seed TEXT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS nonce INTEGER NOT NULL DEFAULT 0")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS server_seed TEXT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS server_seed_hash TEXT")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_spin_at BIGINT")

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
                # cases + mapping
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cases (
                      id BIGSERIAL PRIMARY KEY,
                      name TEXT NOT NULL,
                      image_url TEXT,
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
                      sort_order INTEGER NOT NULL DEFAULT 0,
                      created_at BIGINT NOT NULL,
                      PRIMARY KEY (case_id, prize_id)
                    )
                    """
                )
                
# If case_prizes existed before (old schema), ensure columns exist before indexes
cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0")
cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE")
cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS weight INTEGER NOT NULL DEFAULT 1")
cur.execute("ALTER TABLE case_prizes ADD COLUMN IF NOT EXISTS created_at BIGINT NOT NULL DEFAULT 0")

                cur.execute("CREATE INDEX IF NOT EXISTS idx_case_prizes_case_active_sort ON case_prizes(case_id, is_active, sort_order, prize_id)")


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
                

                # provably-fair fields for spins (non-breaking; added as nullable)
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS nonce INTEGER")
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS client_seed TEXT")
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS server_seed TEXT")
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS server_seed_hash TEXT")
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS rng_hex TEXT")
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
                # track case used for the spin (optional for older deployments)
                cur.execute("ALTER TABLE spins ADD COLUMN IF NOT EXISTS case_id BIGINT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_spins_case_time ON spins(case_id, created_at)")

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

                # accounting / idempotency / withdrawals
                cur.execute(
    """
    CREATE TABLE IF NOT EXISTS ledger (
      id BIGSERIAL PRIMARY KEY,
      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
      delta INTEGER NOT NULL,
      reason TEXT NOT NULL,
      ref TEXT,
      created_at BIGINT NOT NULL
    )
    """
)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ledger_user_time ON ledger(tg_user_id, created_at)")

                cur.execute(
    """
    CREATE TABLE IF NOT EXISTS idempotency (
      tg_user_id TEXT NOT NULL,
      key TEXT NOT NULL,
      response JSONB NOT NULL,
      created_at BIGINT NOT NULL,
      PRIMARY KEY (tg_user_id, key)
    )
    """
)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_idem_time ON idempotency(created_at)")

                cur.execute(
    """
    CREATE TABLE IF NOT EXISTS withdrawals (
      id BIGSERIAL PRIMARY KEY,
      tg_user_id TEXT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
      inventory_id BIGINT NOT NULL,
      prize_id BIGINT NOT NULL,
      gift_id TEXT NOT NULL,
      status TEXT NOT NULL,
      telegram_result TEXT,
      created_at BIGINT NOT NULL,
      updated_at BIGINT
    )
    """
)
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_withdrawals_inventory ON withdrawals(inventory_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_withdrawals_status_time ON withdrawals(status, created_at)")
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
                # seed cases if empty (defaults for backward-compat)
                cur.execute("SELECT COUNT(*) FROM cases")
                cc = int(cur.fetchone()[0] or 0)
                now = int(time.time())
                if cc == 0:
                    cur.execute(
                        "INSERT INTO cases (id, name, image_url, price, is_active, sort_order, created_at) "
                        "VALUES (1,'Кейс 25⭐',NULL,25,TRUE,0,%s) ON CONFLICT (id) DO NOTHING",
                        (now,),
                    )
                    cur.execute(
                        "INSERT INTO cases (id, name, image_url, price, is_active, sort_order, created_at) "
                        "VALUES (2,'Кейс 50⭐',NULL,50,TRUE,1,%s) ON CONFLICT (id) DO NOTHING",
                        (now,),
                    )

                # ensure mappings exist (for every case & prize)
                cur.execute("SELECT id FROM cases")
                case_ids = [int(r[0]) for r in cur.fetchall()]
                for cid in case_ids:
                    cur.execute(
                        "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, sort_order, created_at) "
                        "SELECT %s, p.id, p.weight, p.is_active, p.sort_order, %s "
                        "FROM prizes p "
                        "ON CONFLICT (case_id, prize_id) DO NOTHING",
                        (cid, now),
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


def _new_server_seed() -> str:
    return secrets.token_hex(32)

def _seed_hash(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()

def ensure_user_fairness(cur, tg_user_id: str):
    """Ensure the user has provably-fair parameters (client seed, nonce, server seed + hash)."""
    cur.execute(
        "SELECT client_seed, nonce, server_seed, server_seed_hash FROM users WHERE tg_user_id=%s",
        (tg_user_id,),
    )
    row = cur.fetchone()
    if not row:
        return

    client_seed, nonce, server_seed, server_seed_hash = row
    updates = {}

    if client_seed is None or str(client_seed).strip() == "":
        updates["client_seed"] = secrets.token_hex(16)

    if nonce is None:
        updates["nonce"] = 0

    if server_seed is None or str(server_seed).strip() == "":
        ss = _new_server_seed()
        updates["server_seed"] = ss
        updates["server_seed_hash"] = _seed_hash(ss)
    elif server_seed_hash is None or str(server_seed_hash).strip() == "":
        updates["server_seed_hash"] = _seed_hash(str(server_seed))

    if updates:
        sets = ", ".join([f"{k} = %s" for k in updates.keys()])
        cur.execute(
            f"UPDATE users SET {sets} WHERE tg_user_id = %s",
            (*updates.values(), tg_user_id),
        )

def require_not_banned(cur, tg_user_id: str):
    cur.execute("SELECT COALESCE(banned, FALSE) FROM users WHERE tg_user_id=%s", (tg_user_id,))
    row = cur.fetchone()
    if row and bool(row[0]):
        raise HTTPException(status_code=403, detail="user is banned")

def _get_idempotency_key(request: Request) -> str:
    key = (request.headers.get("Idempotency-Key") or request.headers.get("X-Idempotency-Key") or "").strip()
    return key[:128] if key else ""

def _idem_advisory_lock(cur, tg_user_id: str, key: str):
    # lock for the duration of the current transaction
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s)::bigint)", (f"{tg_user_id}:{key}",))

def _idem_get(cur, tg_user_id: str, key: str):
    cur.execute("SELECT response FROM idempotency WHERE tg_user_id=%s AND key=%s", (tg_user_id, key))
    row = cur.fetchone()
    return row[0] if row else None

def _idem_put(cur, tg_user_id: str, key: str, response_obj: dict):
    cur.execute(
        "INSERT INTO idempotency (tg_user_id, key, response, created_at) "
        "VALUES (%s,%s,%s::jsonb,%s) "
        "ON CONFLICT (tg_user_id, key) DO UPDATE SET response = EXCLUDED.response",
        (tg_user_id, key, json.dumps(response_obj, ensure_ascii=False), int(time.time())),
    )

def ledger_add(cur, tg_user_id: str, delta: int, reason: str, ref: str | None = None):
    cur.execute(
        "INSERT INTO ledger (tg_user_id, delta, reason, ref, created_at) VALUES (%s,%s,%s,%s,%s)",
        (tg_user_id, int(delta), str(reason), (str(ref) if ref is not None else None), int(time.time())),
    )

def cleanup_idempotency(cur):
    # opportunistic cleanup, bounded by TTL; safe to call occasionally
    ttl = int(IDEMPOTENCY_TTL_SEC)
    if ttl <= 0:
        return
    cutoff = int(time.time()) - ttl
    cur.execute("DELETE FROM idempotency WHERE created_at < %s", (cutoff,))

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

    ensure_user_fairness(cur, tg_user_id)

    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (tg_user_id,))
    row = cur.fetchone()
    return int(row[0]) if row else START_BALANCE



def get_balance(cur, tg_user_id: str) -> int:
    """Return user's current balance from DB. Assumes user exists."""
    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (tg_user_id,))
    row = cur.fetchone()
    # Should exist because get_or_create_user() is called before most endpoints
    return int(row[0]) if row and row[0] is not None else 0



def fetch_active_cases(cur) -> list[dict]:
    cur.execute(
        "SELECT id, name, image_url, price, is_active, sort_order, created_at "
        "FROM cases "
        "WHERE is_active = TRUE "
        "ORDER BY sort_order ASC, id ASC"
    )
    rows = cur.fetchall()
    return [
        {
            "id": int(r[0]),
            "name": str(r[1]),
            "image_url": (str(r[2]).strip() if r[2] is not None and str(r[2]).strip() else None),
            "price": int(r[3]),
            "is_active": bool(r[4]),
            "sort_order": int(r[5]),
            "created_at": int(r[6] or 0),
        }
        for r in rows
    ]


def fetch_active_prizes(cur, case_id: Optional[int] = None) -> list[dict]:
    if case_id is None:
        cur.execute(
            "SELECT id, name, icon_url, cost, weight FROM prizes "
            "WHERE is_active = TRUE AND weight > 0 "
            "ORDER BY sort_order ASC, id ASC"
        )
        rows = cur.fetchall()
        return [
            {
                "id": int(r[0]),
                "name": str(r[1]),
                "icon_url": (str(r[2]).strip() if r[2] is not None and str(r[2]).strip() else None),
                "cost": int(r[3]),
                "weight": int(r[4]),
            }
            for r in rows
        ]

    cur.execute(
        "SELECT p.id, p.name, p.icon_url, p.cost, cp.weight "
        "FROM case_prizes cp "
        "JOIN prizes p ON p.id = cp.prize_id "
        "WHERE cp.case_id = %s AND cp.is_active = TRUE AND cp.weight > 0 AND p.is_active = TRUE "
        "ORDER BY cp.sort_order ASC, p.sort_order ASC, p.id ASC",
        (int(case_id),),
    )
    rows = cur.fetchall()
    return [
        {
            "id": int(r[0]),
            "name": str(r[1]),
            "icon_url": (str(r[2]).strip() if r[2] is not None and str(r[2]).strip() else None),
            "cost": int(r[3]),
            "weight": int(r[4]),
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

@app.post("/fairness")
def fairness_state(req: FairnessStateReq):
    """Return current provably-fair commitment (server_seed_hash) and client_seed/nonce."""
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                ensure_user_fairness(cur, uid)
                cur.execute(
                    "SELECT COALESCE(client_seed,''), COALESCE(nonce,0), COALESCE(server_seed_hash,'') "
                    "FROM users WHERE tg_user_id=%s",
                    (uid,),
                )
                client_seed, nonce, server_seed_hash = cur.fetchone()
    return {"ok": True, "client_seed": str(client_seed or ""), "nonce": int(nonce or 0), "server_seed_hash": str(server_seed_hash or "")}


@app.post("/fairness/set_client_seed")
def fairness_set_client_seed(req: FairnessSetClientSeedReq):
    """Set client seed used in provably-fair RNG. Resets nonce to 0."""
    uid = extract_tg_user_id(req.initData)
    seed = (req.client_seed or "").strip()
    if not seed:
        raise HTTPException(status_code=400, detail="client_seed is required")
    if len(seed) > MAX_CLIENT_SEED_LEN:
        seed = seed[:MAX_CLIENT_SEED_LEN]
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                require_not_banned(cur, uid)
                ensure_user_fairness(cur, uid)
                cur.execute(
                    "UPDATE users SET client_seed=%s, nonce=0 WHERE tg_user_id=%s",
                    (seed, uid),
                )
                cur.execute(
                    "SELECT COALESCE(server_seed_hash,'') FROM users WHERE tg_user_id=%s",
                    (uid,),
                )
                server_seed_hash = str(cur.fetchone()[0] or "")
    return {"ok": True, "client_seed": seed, "nonce": 0, "server_seed_hash": server_seed_hash}








@app.post("/cases")
def cases(req: CasesReq):
    """Public list of active cases for the frontend."""
    uid = extract_tg_user_id(req.initData)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                items = fetch_active_cases(cur)
    return {"items": items}



@app.post("/prizes")
def prizes(req: PrizesReq):
    """
    Public list of active prizes for the frontend (roulette icons, prices).

    If case_id is provided, returns prizes configured for that case (case_prizes.weight).
    """
    uid = extract_tg_user_id(req.initData)
    case_id = int(req.case_id) if req.case_id is not None else None

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)

                if case_id is None:
                    cur.execute(
                        "SELECT id, name, cost, icon_url "
                        "FROM prizes WHERE is_active = TRUE AND weight > 0 "
                        "ORDER BY sort_order ASC, id ASC"
                    )
                    rows = cur.fetchall()
                    items = []
                    for r in rows:
                        items.append(
                            {
                                "id": int(r[0]),
                                "name": str(r[1]),
                                "cost": int(r[2]),
                                "icon_url": (str(r[3]).strip() if r[3] is not None and str(r[3]).strip() else None),
                            }
                        )
                    return {"items": items}

                cur.execute(
                    "SELECT p.id, p.name, p.cost, p.icon_url, cp.weight "
                    "FROM case_prizes cp "
                    "JOIN prizes p ON p.id = cp.prize_id "
                    "WHERE cp.case_id = %s AND cp.is_active = TRUE AND cp.weight > 0 AND p.is_active = TRUE "
                    "ORDER BY cp.sort_order ASC, p.sort_order ASC, p.id ASC",
                    (case_id,),
                )
                rows = cur.fetchall()

    items = []
    for r in rows:
        items.append(
            {
                "id": int(r[0]),
                "name": str(r[1]),
                "cost": int(r[2]),
                "icon_url": (str(r[3]).strip() if r[3] is not None and str(r[3]).strip() else None),
                "weight": int(r[4]),
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
def inventory_sell(request: Request, req: InventorySellReq):
    uid = extract_tg_user_id(req.initData)
    inv_id = int(req.inventory_id)
    idem_key = _get_idempotency_key(request)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                require_not_banned(cur, uid)

                # resolve case -> authoritative bet cost
                if case_id is not None:
                    cur.execute("SELECT id, price FROM cases WHERE id=%s AND is_active=TRUE", (case_id,))
                    row = cur.fetchone()
                    if not row:
                        raise HTTPException(status_code=404, detail="case not found")
                    case_id = int(row[0])
                    cost = int(row[1])
                else:
                    # backward-compat: map cost to a case by price if possible
                    cur.execute(
                        "SELECT id, price FROM cases WHERE price=%s AND is_active=TRUE ORDER BY sort_order ASC, id ASC LIMIT 1",
                        (int(cost),),
                    )
                    row = cur.fetchone()
                    if row:
                        case_id = int(row[0])
                        cost = int(row[1])
                    else:
                        raise HTTPException(status_code=400, detail="bad cost")

                if idem_key:
                    _idem_advisory_lock(cur, uid, idem_key)
                    cached = _idem_get(cur, uid, idem_key)
                    if cached:
                        return cached

                cur.execute(
                    "SELECT prize_cost, COALESCE(is_locked, FALSE) "
                    "FROM inventory WHERE id=%s AND tg_user_id=%s FOR UPDATE",
                    (inv_id, uid),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="inventory item not found")

                prize_cost = int(row[0] or 0)
                if bool(row[1]):
                    raise HTTPException(status_code=409, detail="item is locked")

                cur.execute("DELETE FROM inventory WHERE id=%s AND tg_user_id=%s", (inv_id, uid))
                cur.execute(
                    "UPDATE users SET balance=balance+%s WHERE tg_user_id=%s RETURNING balance",
                    (prize_cost, uid),
                )
                bal = int(cur.fetchone()[0])
                ledger_add(cur, uid, prize_cost, "inventory_sell", ref=str(inv_id))

                resp = {"ok": True, "inventory_id": inv_id, "credited": prize_cost, "balance": bal}
                if idem_key:
                    _idem_put(cur, uid, idem_key, resp)
                # cleanup occasionally, to keep the table bounded
                if random.random() < 0.02:
                    cleanup_idempotency(cur)
                return resp

@app.post("/inventory/withdraw")
def inventory_withdraw(request: Request, req: InventoryWithdrawReq):
    """
    Withdraw inventory item:

    - Regular prize (is_unique = FALSE): create a withdrawal row (status=sending), then bot sends gift via sendGift,
      then mark withdrawal as sent and delete item from inventory.
      This prevents duplicate sends on retries: if a withdrawal is already 'sending' or 'sent', we do not send again.

    - Unique prize (is_unique = TRUE): create a claim for admins and lock the inventory item.
    """
    uid = extract_tg_user_id(req.initData)
    inv_id = int(req.inventory_id)
    idem_key = _get_idempotency_key(request)

    # Gift info used outside the first transaction
    gift_id: str | None = None
    is_unique: bool = False

    with pool.connection() as con:
        # --- TX1: lock inventory + decide flow + create claim/withdrawal row ---
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                require_not_banned(cur, uid)

                if idem_key:
                    _idem_advisory_lock(cur, uid, idem_key)
                    cached = _idem_get(cur, uid, idem_key)
                    if cached:
                        return cached

                # lock inventory row to avoid double-withdraw/sell
                cur.execute(
                    "SELECT id, prize_id, prize_name, prize_cost, COALESCE(is_locked, FALSE) AS is_locked "
                    "FROM inventory WHERE id=%s AND tg_user_id=%s FOR UPDATE",
                    (inv_id, uid),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="inventory item not found")
                _inv_id, prize_id, prize_name, prize_cost, locked = row

                # prize attributes
                cur.execute(
                    "SELECT COALESCE(is_unique, FALSE) AS is_unique, COALESCE(gift_id, '') AS gift_id "
                    "FROM prizes WHERE id=%s",
                    (int(prize_id),),
                )
                prow = cur.fetchone()
                if prow:
                    is_unique = bool(prow[0])
                    gift_id = str(prow[1] or "").strip()
                else:
                    is_unique = False
                    gift_id = ""

                if locked:
                    # idempotent responses
                    cur.execute(
                        "SELECT id, status FROM claims WHERE inventory_id=%s ORDER BY created_at DESC LIMIT 1",
                        (inv_id,),
                    )
                    c = cur.fetchone()
                    if c:
                        resp = {"ok": True, "status": str(c[1]), "message": "Заявка уже существует."}
                        if idem_key:
                            _idem_put(cur, uid, idem_key, resp)
                        return resp

                    cur.execute(
                        "SELECT status FROM withdrawals WHERE inventory_id=%s ORDER BY created_at DESC LIMIT 1",
                        (inv_id,),
                    )
                    w = cur.fetchone()
                    if w:
                        st = str(w[0])
                        resp = {"ok": True, "status": st, "message": "Вывод уже в обработке." if st == "sending" else "Подарок уже отправлен."}
                        if idem_key:
                            _idem_put(cur, uid, idem_key, resp)
                        return resp

                    raise HTTPException(status_code=409, detail="item is locked")

                if is_unique:
                    now = int(time.time())
                    cur.execute(
                        "INSERT INTO claims (tg_user_id, inventory_id, prize_id, prize_name, prize_cost, status, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,'pending',%s) RETURNING id",
                        (uid, inv_id, int(prize_id), str(prize_name), int(prize_cost), now),
                    )
                    claim_id = int(cur.fetchone()[0])
                    cur.execute(
                        "UPDATE inventory SET is_locked = TRUE, locked_reason = 'claim_pending' WHERE id=%s",
                        (inv_id,),
                    )
                    resp = {"ok": True, "status": "pending", "claim_id": claim_id, "message": "Заявка на уникальный подарок создана."}
                    if idem_key:
                        _idem_put(cur, uid, idem_key, resp)
                    return resp

                # regular gifts
                if not gift_id:
                    raise HTTPException(status_code=400, detail="gift_id is not configured for this prize")

                # prevent double-send via withdrawal row (unique per inventory_id)
                now = int(time.time())
                cur.execute("SELECT status FROM withdrawals WHERE inventory_id=%s FOR UPDATE", (inv_id,))
                w = cur.fetchone()
                if w:
                    st = str(w[0])
                    if st == "sent":
                        # inventory should already be deleted, but be tolerant
                        cur.execute("DELETE FROM inventory WHERE id=%s AND tg_user_id=%s", (inv_id, uid))
                        resp = {"ok": True, "status": "sent", "message": "Подарок уже отправлен."}
                        if idem_key:
                            _idem_put(cur, uid, idem_key, resp)
                        return resp
                    if st == "sending":
                        resp = {"ok": True, "status": "sending", "message": "Отправка в процессе. Попробуйте позже."}
                        if idem_key:
                            _idem_put(cur, uid, idem_key, resp)
                        return resp
                    # failed -> allow retry
                    cur.execute("UPDATE withdrawals SET status='sending', updated_at=%s WHERE inventory_id=%s", (now, inv_id))
                else:
                    cur.execute(
                        "INSERT INTO withdrawals (tg_user_id, inventory_id, prize_id, gift_id, status, created_at) "
                        "VALUES (%s,%s,%s,%s,'sending',%s)",
                        (uid, inv_id, int(prize_id), gift_id, now),
                    )

                # lock inventory while sending
                cur.execute("UPDATE inventory SET is_locked = TRUE, locked_reason = 'withdraw_sending' WHERE id=%s", (inv_id,))

        # --- external call (no DB locks) ---
        # Only real Telegram users can receive gifts
        try:
            user_int = int(uid)
        except Exception:
            # mark failed
            with con:
                with con.cursor() as cur:
                    cur.execute(
                        "UPDATE withdrawals SET status='failed', telegram_result=%s, updated_at=%s WHERE inventory_id=%s",
                        ("invalid user_id", int(time.time()), inv_id),
                    )
                    cur.execute("UPDATE inventory SET is_locked = FALSE, locked_reason = NULL WHERE id=%s", (inv_id,))
            raise HTTPException(status_code=400, detail="cannot withdraw for this user")

        send_ok = False
        send_result = None
        send_err = None
        try:
            send_result = tg_api("sendGift", {"gift_id": gift_id, "user_id": user_int})
            send_ok = True
        except HTTPException as e:
            send_err = str(e.detail)
        except Exception as e:
            send_err = str(e)

        # --- TX2: finalize state ---
        with con:
            with con.cursor() as cur:
                if send_ok:
                    cur.execute(
                        "UPDATE withdrawals SET status='sent', telegram_result=%s, updated_at=%s WHERE inventory_id=%s",
                        (json.dumps(send_result, ensure_ascii=False), int(time.time()), inv_id),
                    )
                    cur.execute("DELETE FROM inventory WHERE id=%s AND tg_user_id=%s", (inv_id, uid))
                    resp = {"ok": True, "status": "sent", "message": "Подарок отправлен ботом."}
                else:
                    cur.execute(
                        "UPDATE withdrawals SET status='failed', telegram_result=%s, updated_at=%s WHERE inventory_id=%s",
                        (send_err or "sendGift failed", int(time.time()), inv_id),
                    )
                    cur.execute("UPDATE inventory SET is_locked = FALSE, locked_reason = NULL WHERE id=%s", (inv_id,))
                    resp = {"ok": False, "status": "failed", "message": "Не удалось отправить подарок. Попробуйте позже."}

                if idem_key:
                    _idem_put(cur, uid, idem_key, resp)
                if random.random() < 0.02:
                    cleanup_idempotency(cur)

        return resp

@app.post("/spin")
def spin(request: Request, req: SpinReq):
    uid = extract_tg_user_id(req.initData)
    case_id = int(req.case_id) if req.case_id is not None else None
    cost = int(req.cost or 25)

    idem_key = _get_idempotency_key(request)
    spin_id = str(uuid.uuid4())
    now = int(time.time())

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                require_not_banned(cur, uid)

                if idem_key:
                    _idem_advisory_lock(cur, uid, idem_key)
                    cached = _idem_get(cur, uid, idem_key)
                    if cached:
                        return cached

                if REQUIRE_CLAIM_BEFORE_NEXT_SPIN:
                    cur.execute(
                        "SELECT spin_id FROM spins WHERE tg_user_id=%s AND status='pending' ORDER BY created_at DESC LIMIT 1",
                        (uid,),
                    )
                    pending = cur.fetchone()
                    if pending:
                        raise HTTPException(status_code=409, detail=f"pending spin exists: {pending[0]}")

                # lock user row to do fair RNG + balance update atomically
                cur.execute(
                    "SELECT balance, COALESCE(nonce,0), COALESCE(client_seed,''), COALESCE(server_seed,''), "
                    "COALESCE(server_seed_hash,''), COALESCE(last_spin_at,0) "
                    "FROM users WHERE tg_user_id=%s FOR UPDATE",
                    (uid,),
                )
                u = cur.fetchone()
                if not u:
                    raise HTTPException(status_code=500, detail="user missing")
                balance, nonce, client_seed, server_seed, server_seed_hash, last_spin_at = u
                balance = int(balance or 0)
                nonce = int(nonce or 0)
                client_seed = str(client_seed or "").strip()
                server_seed = str(server_seed or "").strip()
                server_seed_hash = str(server_seed_hash or "").strip()
                last_spin_at = int(last_spin_at or 0)

                # ensure seeds exist
                if not client_seed or not server_seed or not server_seed_hash:
                    ensure_user_fairness(cur, uid)
                    cur.execute(
                        "SELECT COALESCE(nonce,0), COALESCE(client_seed,''), COALESCE(server_seed,''), COALESCE(server_seed_hash,'') "
                        "FROM users WHERE tg_user_id=%s FOR UPDATE",
                        (uid,),
                    )
                    nonce, client_seed, server_seed, server_seed_hash = cur.fetchone()
                    nonce = int(nonce or 0)
                    client_seed = str(client_seed or "").strip()
                    server_seed = str(server_seed or "").strip()
                    server_seed_hash = str(server_seed_hash or "").strip()

                # cooldown
                if SPIN_COOLDOWN_SEC > 0 and last_spin_at and (now - last_spin_at) < SPIN_COOLDOWN_SEC:
                    raise HTTPException(status_code=429, detail="too many spins; slow down")

                # funds
                if balance < cost:
                    raise HTTPException(status_code=402, detail="not enough balance")

                # provably fair RNG: HMAC_SHA256(server_seed, "{uid}|{client_seed}|{nonce}|{cost}")
                msg = f"{uid}|{client_seed}|{nonce}|{cost}".encode("utf-8")
                digest = hmac.new(server_seed.encode("utf-8"), msg, hashlib.sha256).digest()
                rng_int = int.from_bytes(digest, "big")
                rng_hex = digest.hex()

                prizes = fetch_active_prizes(cur, case_id)
                if not prizes:
                    prizes = [{"id": p["id"], "name": p["name"], "icon_url": (p.get("icon_url") or None), "cost": p["cost"], "weight": p["weight"]} for p in DEFAULT_PRIZES]

                total_w = sum(int(p["weight"]) for p in prizes)
                if total_w <= 0:
                    raise HTTPException(status_code=500, detail="no active prizes")
                pick = rng_int % total_w
                chosen = None
                acc = 0
                for p in prizes:
                    w = int(p["weight"])
                    acc += w
                    if pick < acc:
                        chosen = p
                        break
                if not chosen:
                    chosen = prizes[-1]

                # rotate server seed for next spin (commit via hash)
                next_server_seed = _new_server_seed()
                next_server_seed_hash = _seed_hash(next_server_seed)

                # apply user accounting
                new_balance = balance - cost
                cur.execute(
                    "UPDATE users SET balance=%s, nonce=%s, last_spin_at=%s, server_seed=%s, server_seed_hash=%s "
                    "WHERE tg_user_id=%s",
                    (new_balance, nonce + 1, now, next_server_seed, next_server_seed_hash, uid),
                )

                # persist spin + accounting
                cur.execute(
                    "INSERT INTO spins (spin_id, tg_user_id, case_id, bet_cost, prize_id, prize_name, prize_cost, status, created_at, "
                    "nonce, client_seed, server_seed, server_seed_hash, rng_hex) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s,%s,%s,%s)",
                    (
                        spin_id,
                        uid,
                        (case_id if case_id is not None else None),
                        cost,
                        int(chosen["id"]),
                        str(chosen["name"]),
                        int(chosen["cost"]),
                        now,
                        nonce,
                        client_seed,
                        server_seed,
                        server_seed_hash,
                        rng_hex,
                    ),
                )
                ledger_add(cur, uid, -cost, "spin_cost", ref=spin_id)

                resp = {
                    "spin_id": spin_id,
                    "case_id": case_id,
                    "bet_cost": int(cost),
                    "id": int(chosen["id"]),
                    "name": str(chosen["name"]),
                    "icon_url": (chosen.get("icon_url") or None),
                    "cost": int(chosen["cost"]),
                    "balance": int(new_balance),
                    "fair": {
                        "client_seed": client_seed,
                        "nonce": int(nonce),
                        "server_seed_hash": server_seed_hash,
                        "server_seed": server_seed,
                        "rng_hex": rng_hex,
                        "next_server_seed_hash": next_server_seed_hash,
                    },
                }

                if idem_key:
                    _idem_put(cur, uid, idem_key, resp)
                if random.random() < 0.02:
                    cleanup_idempotency(cur)

                return resp

@app.post("/claim")
def claim(request: Request, req: ClaimReq):
    uid = extract_tg_user_id(req.initData)
    idem_key = _get_idempotency_key(request)

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                require_not_banned(cur, uid)

                if idem_key:
                    _idem_advisory_lock(cur, uid, idem_key)
                    cached = _idem_get(cur, uid, idem_key)
                    if cached:
                        return cached

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
                    resp = {"ok": True, "status": status, "balance": bal}
                    if idem_key:
                        _idem_put(cur, uid, idem_key, resp)
                    return resp

                if req.action == "sell":
                    cur.execute(
                        "UPDATE users SET balance = balance + %s WHERE tg_user_id=%s RETURNING balance",
                        (prize_cost, uid),
                    )
                    bal = int(cur.fetchone()[0])
                    cur.execute("UPDATE spins SET status='sold' WHERE spin_id=%s", (req.spin_id,))
                    ledger_add(cur, uid, prize_cost, "spin_sell", ref=str(req.spin_id))
                    resp = {"ok": True, "status": "sold", "balance": bal, "credited": prize_cost}
                    if idem_key:
                        _idem_put(cur, uid, idem_key, resp)
                    if random.random() < 0.02:
                        cleanup_idempotency(cur)
                    return resp

                # keep
                cur.execute(
                    "INSERT INTO inventory (tg_user_id, prize_id, prize_name, prize_cost, created_at) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (uid, prize_id, prize_name, prize_cost, int(time.time())),
                )
                cur.execute("UPDATE spins SET status='kept' WHERE spin_id=%s", (req.spin_id,))
                cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (uid,))
                bal = int(cur.fetchone()[0])
                resp = {"ok": True, "status": "kept", "balance": bal}
                if idem_key:
                    _idem_put(cur, uid, idem_key, resp)
                if random.random() < 0.02:
                    cleanup_idempotency(cur)
                return resp

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
def topup_create(request: Request, req: TopupCreateReq):
    uid = extract_tg_user_id(req.initData)
    stars = int(req.stars or 0)
    if stars < 1 or stars > 10000:
        raise HTTPException(status_code=400, detail="bad stars amount")

    idem_key = _get_idempotency_key(request)

    # Create unique payload per invoice
    payload = f"topup:{uid}:{uuid.uuid4()}"
    now = int(time.time())

    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                public = extract_tg_user_public(req.initData)
                get_or_create_user(cur, uid, public)
                require_not_banned(cur, uid)

                if idem_key:
                    _idem_advisory_lock(cur, uid, idem_key)
                    cached = _idem_get(cur, uid, idem_key)
                    if cached:
                        return cached

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

    resp = {"invoice_link": invoice_link, "payload": payload, "stars": stars}

    if idem_key:
        with pool.connection() as con:
            with con:
                with con.cursor() as cur:
                    _idem_advisory_lock(cur, uid, idem_key)
                    _idem_put(cur, uid, idem_key, resp)
                    if random.random() < 0.02:
                        cleanup_idempotency(cur)

    return resp

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
                    ledger_add(cur, uid, expected, "topup_paid", ref=invoice_payload)
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
                    ledger_add(cur, uid, expected, "topup_reconcile", ref=payload)
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


@app.post("/admin/ban/{tg_user_id}")
def admin_ban_user(request: Request, tg_user_id: str):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, tg_user_id)
                cur.execute("UPDATE users SET banned=TRUE WHERE tg_user_id=%s", (tg_user_id,))
    return {"ok": True, "tg_user_id": tg_user_id, "banned": True}


@app.post("/admin/unban/{tg_user_id}")
def admin_unban_user(request: Request, tg_user_id: str):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                get_or_create_user(cur, tg_user_id)
                cur.execute("UPDATE users SET banned=FALSE WHERE tg_user_id=%s", (tg_user_id,))
    return {"ok": True, "tg_user_id": tg_user_id, "banned": False}


@app.get("/admin/ledger/{tg_user_id}")
def admin_ledger(request: Request, tg_user_id: str, limit: int = Query(200, ge=1, le=1000)):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT id, delta, reason, ref, created_at FROM ledger "
                    "WHERE tg_user_id=%s ORDER BY id DESC LIMIT %s",
                    (tg_user_id, int(limit)),
                )
                rows = cur.fetchall()
    return {"items": [{"id": int(r[0]), "delta": int(r[1]), "reason": str(r[2]), "ref": (r[3] or None), "created_at": int(r[4])} for r in rows]}


@app.get("/admin/withdrawals")
def admin_withdrawals(request: Request, status: str = Query("", description="sending|sent|failed or empty for all"), limit: int = Query(200, ge=1, le=1000)):
    require_admin(request)
    st = (status or "").strip()
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT w.id, w.tg_user_id, w.inventory_id, w.prize_id, w.gift_id, w.status, w.telegram_result, w.created_at, w.updated_at "
                    "FROM withdrawals w "
                    "WHERE (%s = '' OR w.status = %s) "
                    "ORDER BY w.created_at DESC LIMIT %s",
                    (st, st, int(limit)),
                )
                rows = cur.fetchall()
    return {"items": [{
        "id": int(r[0]),
        "tg_user_id": str(r[1]),
        "inventory_id": int(r[2]),
        "prize_id": int(r[3]),
        "gift_id": str(r[4]),
        "status": str(r[5]),
        "telegram_result": r[6],
        "created_at": int(r[7]),
        "updated_at": int(r[8] or 0) or None,
    } for r in rows]}

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
                ledger_add(cur, uid, delta, "admin_adjust")

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
                # auto-map new prize into all existing cases (default: use prize weight/sort)
                cur.execute("SELECT id FROM cases")
                cids = [int(r[0]) for r in cur.fetchall()]
                for cid in cids:
                    cur.execute(
                        "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, sort_order, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (case_id, prize_id) DO NOTHING",
                        (cid, new_id, int(req.weight), bool(req.is_active), int(req.sort_order or 0), now),
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

# ===== Admin: Cases =====
@app.get("/admin/cases")
def admin_list_cases(request: Request):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT id, name, image_url, price, is_active, sort_order, created_at "
                    "FROM cases ORDER BY sort_order ASC, id ASC"
                )
                rows = cur.fetchall()
    items = []
    for r in rows:
        items.append(
            {
                "id": int(r[0]),
                "name": str(r[1]),
                "image_url": (str(r[2]).strip() if r[2] is not None and str(r[2]).strip() else None),
                "price": int(r[3]),
                "is_active": bool(r[4]),
                "sort_order": int(r[5]),
                "created_at": int(r[6] or 0),
            }
        )
    return {"items": items}


@app.post("/admin/cases")
def admin_create_case(request: Request, case: CaseIn):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                now = int(time.time())
                cur.execute(
                    "INSERT INTO cases (name, image_url, price, is_active, sort_order, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
                    (
                        case.name,
                        (case.image_url.strip() if case.image_url else None),
                        int(case.price),
                        bool(case.is_active),
                        int(case.sort_order or 0),
                        now,
                    ),
                )
                cid = int(cur.fetchone()[0])

                # auto-map all existing prizes with their default weights
                cur.execute(
                    "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, sort_order, created_at) "
                    "SELECT %s, p.id, p.weight, p.is_active, p.sort_order, %s FROM prizes p "
                    "ON CONFLICT (case_id, prize_id) DO NOTHING",
                    (cid, now),
                )

    return {"ok": True, "id": cid}


@app.put("/admin/cases/{case_id}")
def admin_update_case(request: Request, case_id: int, case: CaseIn):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "UPDATE cases SET name=%s, image_url=%s, price=%s, is_active=%s, sort_order=%s "
                    "WHERE id=%s RETURNING id",
                    (
                        case.name,
                        (case.image_url.strip() if case.image_url else None),
                        int(case.price),
                        bool(case.is_active),
                        int(case.sort_order or 0),
                        int(case_id),
                    ),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="case not found")
    return {"ok": True, "id": int(case_id)}


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
def admin_case_prizes(request: Request, case_id: int):
    require_admin(request)
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT p.id, p.name, p.icon_url, p.cost, p.is_active, p.sort_order, "
                    "COALESCE(cp.weight, 0) AS case_weight, COALESCE(cp.is_active, FALSE) AS case_active, "
                    "COALESCE(cp.sort_order, p.sort_order) AS case_sort "
                    "FROM prizes p "
                    "LEFT JOIN case_prizes cp ON cp.prize_id=p.id AND cp.case_id=%s "
                    "ORDER BY case_sort ASC, p.id ASC",
                    (int(case_id),),
                )
                rows = cur.fetchall()
    items = []
    for r in rows:
        items.append(
            {
                "prize_id": int(r[0]),
                "name": str(r[1]),
                "icon_url": (str(r[2]).strip() if r[2] is not None and str(r[2]).strip() else None),
                "cost": int(r[3]),
                "prize_active": bool(r[4]),
                "prize_sort": int(r[5]),
                "weight": int(r[6] or 0),
                "is_active": bool(r[7]),
                "sort_order": int(r[8] or 0),
            }
        )
    return {"items": items}


@app.post("/admin/cases/{case_id}/prizes")
def admin_replace_case_prizes(request: Request, case_id: int, body: CasePrizesReplaceReq):
    require_admin(request)
    items = body.items or []
    with pool.connection() as con:
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT id FROM cases WHERE id=%s", (int(case_id),))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="case not found")

                now = int(time.time())
                cur.execute("DELETE FROM case_prizes WHERE case_id=%s", (int(case_id),))
                for it in items:
                    cur.execute(
                        "INSERT INTO case_prizes (case_id, prize_id, weight, is_active, sort_order, created_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            int(case_id),
                            int(it.prize_id),
                            int(it.weight),
                            bool(it.is_active),
                            int(it.sort_order or 0),
                            now,
                        ),
                    )
    return {"ok": True}






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
