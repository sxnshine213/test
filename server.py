import os
import time
import json
import hmac
import hashlib
import random
from typing import Any, Dict, Optional, List

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg_pool import ConnectionPool


# -------------------------
# Config
# -------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
ALLOW_GUEST = os.environ.get("ALLOW_GUEST", "0") == "1"
START_BALANCE = int(os.environ.get("START_BALANCE", "0"))

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

API_BASE_TG = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""


pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=int(os.environ.get("PG_MAX_SIZE", "5")))


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------
# Models
# -------------------------
class InitOnly(BaseModel):
    initData: str = ""


class CasePrizesReq(BaseModel):
    initData: str = ""
    case_id: int


class SpinReq(BaseModel):
    initData: str = ""
    case_id: int


class InventoryIdReq(BaseModel):
    initData: str = ""
    inventory_id: int


# -------------------------
# Telegram WebApp initData verification
# -------------------------
def _parse_init_data(init_data: str) -> Dict[str, str]:
    # initData is querystring: key=value&key=value...
    out: Dict[str, str] = {}
    for part in init_data.split("&"):
        if not part:
            continue
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k] = v
    return out


def _verify_init_data(init_data: str) -> Dict[str, Any]:
    """
    Verify Telegram WebApp initData signature.
    Returns parsed user dict and auth_date if valid.
    """
    if not init_data:
        if ALLOW_GUEST:
            return {"tg_user_id": 0, "user": {"id": 0, "first_name": "Guest"}}
        raise HTTPException(status_code=401, detail="initData is required")

    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not configured")

    parsed = _parse_init_data(init_data)
    their_hash = parsed.get("hash", "")
    if not their_hash:
        raise HTTPException(status_code=401, detail="initData hash is missing")

    # build data_check_string
    pairs = []
    for k in sorted(parsed.keys()):
        if k == "hash":
            continue
        pairs.append(f"{k}={parsed[k]}")
    data_check_string = "\n".join(pairs)

    secret_key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc_hash, their_hash):
        raise HTTPException(status_code=401, detail="initData signature is invalid")

    user_json = parsed.get("user")
    if not user_json:
        # Some contexts might not include user; but in MiniApp it should.
        raise HTTPException(status_code=401, detail="initData user is missing")

    try:
        user = json.loads(_url_unquote(user_json))
    except Exception:
        # Try raw (already unquoted)
        try:
            user = json.loads(user_json)
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"initData user parse error: {e}")

    return {"tg_user_id": int(user["id"]), "user": user, "auth_date": int(parsed.get("auth_date", "0") or 0)}


def _url_unquote(s: str) -> str:
    # Minimal percent-decoding (no plus-to-space, because initData uses % encoding)
    from urllib.parse import unquote
    return unquote(s)


# -------------------------
# DB helpers
# -------------------------
def migrate(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            tg_user_id BIGINT PRIMARY KEY,
            balance BIGINT NOT NULL DEFAULT 0,
            created_at BIGINT NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS prizes(
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            icon_url TEXT,
            cost INT NOT NULL DEFAULT 0,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            sort_order INT NOT NULL DEFAULT 0,
            gift_id TEXT,
            is_unique BOOLEAN NOT NULL DEFAULT FALSE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cases(
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            price INT NOT NULL DEFAULT 0,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            sort_order INT NOT NULL DEFAULT 0
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS case_prizes(
            case_id INT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
            prize_id INT NOT NULL REFERENCES prizes(id) ON DELETE CASCADE,
            weight INT NOT NULL DEFAULT 1,
            PRIMARY KEY(case_id, prize_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory(
            id SERIAL PRIMARY KEY,
            tg_user_id BIGINT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
            prize_id INT NOT NULL REFERENCES prizes(id),
            prize_name TEXT NOT NULL,
            prize_cost INT NOT NULL,
            icon_url TEXT,
            created_at BIGINT NOT NULL,
            is_locked BOOLEAN NOT NULL DEFAULT FALSE,
            locked_reason TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS claims(
            id SERIAL PRIMARY KEY,
            tg_user_id BIGINT NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
            inventory_id INT NOT NULL REFERENCES inventory(id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at BIGINT NOT NULL,
            processed_at BIGINT
        );
        """)
    conn.commit()


def ensure_user(conn, tg_user_id: int) -> None:
    now = int(time.time())
    with conn.cursor() as cur:
        cur.execute("SELECT tg_user_id FROM users WHERE tg_user_id::bigint=%s", (int(tg_user_id),))
        row = cur.fetchone()
        if row:
            return
        cur.execute(
            "INSERT INTO users (tg_user_id, balance, created_at) VALUES (%s,%s,%s)",
            (tg_user_id, START_BALANCE, now),
        )
    conn.commit()


def get_balance(cur, tg_user_id: int) -> int:
    cur.execute("SELECT balance FROM users WHERE tg_user_id=%s", (tg_user_id,))
    r = cur.fetchone()
    return int(r[0]) if r else 0


def set_balance(cur, tg_user_id: int, new_balance: int) -> None:
    cur.execute("UPDATE users SET balance=%s WHERE tg_user_id=%s", (int(new_balance), int(tg_user_id)))


def now_ts() -> int:
    return int(time.time())


# -------------------------
# Telegram API helper
# -------------------------
def tg_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not API_BASE_TG:
        raise HTTPException(status_code=500, detail="BOT_TOKEN is not configured")
    url = f"{API_BASE_TG}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=10)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"telegram network error: {e}")
    try:
        data = resp.json()
    except Exception:
        raise HTTPException(status_code=502, detail=f"telegram bad response: {resp.text[:200]}")
    if not data.get("ok"):
        desc = data.get("description", "telegram error")
        code = data.get("error_code", 400)
        # keep Telegram code in message, but map to 400/403/502 appropriately
        if code in (401, 403):
            raise HTTPException(status_code=403, detail=f"telegram: {desc}")
        raise HTTPException(status_code=400, detail=f"telegram: {desc}")
    return data["result"]


# -------------------------
# App lifecycle
# -------------------------
@app.on_event("startup")
def _startup() -> None:
    with pool.connection() as conn:
        migrate(conn)
        # Optional: create a default case if none exists
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM cases LIMIT 1")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO cases (name, description, price, is_active, sort_order) VALUES (%s,%s,%s,%s,%s) RETURNING id",
                    ("Базовый кейс", "Стартовый набор призов", 25, True, 0),
                )
                case_id = cur.fetchone()[0]
                # Attach all active prizes if any exist (weight=1)
                cur.execute("SELECT id FROM prizes WHERE is_active=TRUE")
                prize_ids = [r[0] for r in cur.fetchall()]
                for pid in prize_ids:
                    cur.execute(
                        "INSERT INTO case_prizes (case_id, prize_id, weight) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        (case_id, pid, 1),
                    )
        conn.commit()


# -------------------------
# Public endpoints
# -------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "random-gift"}


@app.post("/me")
def me(req: InitOnly):
    info = _verify_init_data(req.initData)
    uid = int(info["tg_user_id"])
    with pool.connection() as conn:
        ensure_user(conn, uid)
        with conn.cursor() as cur:
            bal = get_balance(cur, uid)
        return {"tg_user_id": uid, "balance": bal}


@app.post("/cases")
def list_cases(req: InitOnly):
    _ = _verify_init_data(req.initData)
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, description, price, is_active, sort_order FROM cases WHERE is_active=TRUE ORDER BY sort_order ASC, id ASC"
            )
            items = []
            for r in cur.fetchall():
                items.append(
                    {"id": int(r[0]), "name": r[1], "description": r[2], "price": int(r[3]), "is_active": bool(r[4]), "sort_order": int(r[5])}
                )
        return {"items": items}


@app.post("/case_prizes")
def case_prizes(req: CasePrizesReq):
    _ = _verify_init_data(req.initData)
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.name, p.icon_url, p.cost, cp.weight, p.is_unique, p.gift_id
                FROM case_prizes cp
                JOIN prizes p ON p.id = cp.prize_id
                WHERE cp.case_id=%s AND p.is_active=TRUE AND cp.weight>0
                ORDER BY p.sort_order ASC, p.id ASC
                """,
                (int(req.case_id),),
            )
            items = []
            for r in cur.fetchall():
                items.append(
                    {
                        "id": int(r[0]),
                        "name": r[1],
                        "icon_url": r[2],
                        "cost": int(r[3]),
                        "weight": int(r[4]),
                        "is_unique": bool(r[5]),
                        "gift_id": r[6],
                    }
                )
        return {"items": items}


@app.post("/spin")
def spin(req: SpinReq):
    info = _verify_init_data(req.initData)
    uid = int(info["tg_user_id"])
    with pool.connection() as conn:
        ensure_user(conn, uid)
        with conn.cursor() as cur:
            # get case price
            cur.execute("SELECT price, is_active FROM cases WHERE id=%s", (int(req.case_id),))
            row = cur.fetchone()
            if not row or not bool(row[1]):
                raise HTTPException(status_code=404, detail="Case not found")
            price = int(row[0])

            bal = get_balance(cur, uid)
            if bal < price:
                raise HTTPException(status_code=400, detail="Insufficient balance")

            # load prize pool for this case
            cur.execute(
                """
                SELECT p.id, p.name, p.icon_url, p.cost, cp.weight
                FROM case_prizes cp
                JOIN prizes p ON p.id = cp.prize_id
                WHERE cp.case_id=%s AND p.is_active=TRUE AND cp.weight>0
                """,
                (int(req.case_id),),
            )
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=400, detail="No prizes configured for this case")

            prize_ids = [int(r[0]) for r in rows]
            weights = [int(r[4]) for r in rows]
            idx = random.choices(range(len(rows)), weights=weights, k=1)[0]
            pr = rows[idx]
            prize_id = int(pr[0])
            prize_name = pr[1]
            icon_url = pr[2]
            prize_cost = int(pr[3])

            # debit and insert inventory
            set_balance(cur, uid, bal - price)
            cur.execute(
                """
                INSERT INTO inventory (tg_user_id, prize_id, prize_name, prize_cost, icon_url, created_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (uid, prize_id, prize_name, prize_cost, icon_url, now_ts()),
            )
            inv_id = int(cur.fetchone()[0])
            new_bal = bal - price

        conn.commit()
        return {"ok": True, "balance": new_bal, "inventory_id": inv_id, "prize": {"id": prize_id, "name": prize_name, "cost": prize_cost, "icon_url": icon_url}}


@app.post("/inventory")
def inventory(req: InitOnly):
    info = _verify_init_data(req.initData)
    uid = int(info["tg_user_id"])
    with pool.connection() as conn:
        ensure_user(conn, uid)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.id, i.prize_name, i.prize_cost, i.icon_url, i.created_at, i.is_locked,
                       p.is_unique, p.gift_id
                FROM inventory i
                JOIN prizes p ON p.id = i.prize_id
                WHERE i.tg_user_id=%s
                ORDER BY i.id DESC
                """,
                (uid,),
            )
            items = []
            for r in cur.fetchall():
                items.append(
                    {
                        "inventory_id": int(r[0]),
                        "name": r[1],
                        "cost": int(r[2]),
                        "icon_url": r[3],
                        "created_at": int(r[4]),
                        "is_locked": bool(r[5]),
                        "is_unique": bool(r[6]),
                        "gift_id": r[7],
                    }
                )
        return {"items": items}


@app.post("/inventory/sell")
def inventory_sell(req: InventoryIdReq):
    info = _verify_init_data(req.initData)
    uid = int(info["tg_user_id"])
    with pool.connection() as conn:
        ensure_user(conn, uid)
        with conn.cursor() as cur:
            # lock inventory row
            cur.execute(
                "SELECT id, prize_cost, is_locked FROM inventory WHERE id=%s AND tg_user_id=%s FOR UPDATE",
                (int(req.inventory_id), uid),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Item not found")
            if bool(row[2]):
                raise HTTPException(status_code=400, detail="Item is locked")

            prize_cost = int(row[1])
            bal = get_balance(cur, uid)

            cur.execute("DELETE FROM inventory WHERE id=%s AND tg_user_id=%s", (int(req.inventory_id), uid))
            set_balance(cur, uid, bal + prize_cost)
            new_bal = bal + prize_cost
        conn.commit()
        return {"ok": True, "balance": new_bal}


@app.post("/inventory/withdraw")
def inventory_withdraw(req: InventoryIdReq):
    info = _verify_init_data(req.initData)
    uid = int(info["tg_user_id"])
    with pool.connection() as conn:
        ensure_user(conn, uid)
        with conn.cursor() as cur:
            # lock inventory row first (no joins in FOR UPDATE)
            cur.execute(
                "SELECT id, prize_id, prize_name, prize_cost, is_locked FROM inventory WHERE id=%s AND tg_user_id=%s FOR UPDATE",
                (int(req.inventory_id), uid),
            )
            inv = cur.fetchone()
            if not inv:
                raise HTTPException(status_code=404, detail="Item not found")
            if bool(inv[4]):
                raise HTTPException(status_code=400, detail="Item is locked")

            prize_id = int(inv[1])
            prize_name = inv[2]
            prize_cost = int(inv[3])

            # fetch prize metadata
            cur.execute("SELECT is_unique, gift_id FROM prizes WHERE id=%s", (prize_id,))
            pr = cur.fetchone()
            if not pr:
                raise HTTPException(status_code=404, detail="Prize not found")
            is_unique = bool(pr[0])
            gift_id = pr[1]

            # mark locked to avoid double-clicks
            cur.execute(
                "UPDATE inventory SET is_locked=TRUE, locked_reason=%s WHERE id=%s AND tg_user_id=%s",
                ("withdraw", int(req.inventory_id), uid),
            )
            bal = get_balance(cur, uid)
        conn.commit()

    # Outside transaction: perform external Telegram API call (avoid keeping DB locks)
    if is_unique:
        with pool.connection() as conn2:
            with conn2.cursor() as cur2:
                cur2.execute(
                    "INSERT INTO claims (tg_user_id, inventory_id, status, created_at) VALUES (%s,%s,%s,%s)",
                    (uid, int(req.inventory_id), "pending", now_ts()),
                )
            conn2.commit()
        return {"ok": True, "message": "Заявка на уникальный подарок создана. Администратор обработает её.", "balance": bal}

    if not gift_id:
        # unlock
        with pool.connection() as conn3:
            with conn3.cursor() as cur3:
                cur3.execute("UPDATE inventory SET is_locked=FALSE, locked_reason=NULL WHERE id=%s AND tg_user_id=%s", (int(req.inventory_id), uid))
            conn3.commit()
        raise HTTPException(status_code=400, detail="gift_id is not configured for this prize")

    # Send gift
    tg_api("sendGift", {"user_id": uid, "gift_id": str(gift_id)})

    # On success, delete inventory item
    with pool.connection() as conn4:
        with conn4.cursor() as cur4:
            cur4.execute("DELETE FROM inventory WHERE id=%s AND tg_user_id=%s", (int(req.inventory_id), uid))
        conn4.commit()
    return {"ok": True, "message": "Подарок отправлен в Telegram.", "balance": bal}
