import os
import json
import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

APP_TITLE = "Yalo Prices API (Demo)"
DB_PATH = os.getenv("DB_PATH", "db.sqlite3")
EXCEL_PATH = os.getenv("EXCEL_PATH", "prices.xlsx")

BASIC_USER = os.getenv("API_BASIC_USER", "yalo-demo")
BASIC_PASS = os.getenv("API_BASIC_PASSWORD", "change-me")

security = HTTPBasic()
app = FastAPI(title=APP_TITLE)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL
        )
    """)
    return conn


def require_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username, BASIC_USER)
    ok_pass = secrets.compare_digest(credentials.password, BASIC_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


def normalize_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "sim")


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    # Campos esperados (padrão do teu print)
    sku = str(row.get("sku", "")).strip()
    key = str(row.get("key", "")).strip()
    if not sku or not key:
        raise ValueError("sku and key are required")

    price = row.get("price", None)
    if price is None or str(price).strip() == "":
        raise ValueError("price is required")
    price = float(price)

    is_active = normalize_bool(row.get("isActive", True))

    return {
        "sku": sku,
        "key": key,
        "price": price,
        "isActive": is_active
    }


def make_id(sku: str, key: str) -> str:
    # ID composto (SKU + key)
    return f"{sku}::{key}"


def load_from_excel_if_exists():
    if not os.path.exists(EXCEL_PATH):
        return

    df = pd.read_excel(EXCEL_PATH)
    # normaliza nomes de colunas
    df.columns = [str(c).strip() for c in df.columns]

    conn = get_db()
    try:
        for _, r in df.iterrows():
            row = {k: r.get(k) for k in df.columns}
            data = normalize_row(row)
            pid = make_id(data["sku"], data["key"])
            conn.execute(
                "INSERT OR REPLACE INTO prices (id, data) VALUES (?, ?)",
                (pid, json.dumps(data, ensure_ascii=False))
            )
        conn.commit()
    finally:
        conn.close()


@app.on_event("startup")
def startup_event():
    load_from_excel_if_exists()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/prices", dependencies=[Depends(require_basic_auth)])
def list_prices() -> List[Dict[str, Any]]:
    conn = get_db()
    try:
        cur = conn.execute("SELECT data FROM prices")
        rows = [json.loads(r[0]) for r in cur.fetchall()]
        # opcional: ordenar
        rows.sort(key=lambda x: (x.get("sku", ""), x.get("key", "")))
        return rows
    finally:
        conn.close()


@app.put("/prices/{sku}/{key}", dependencies=[Depends(require_basic_auth)])
def upsert_price(sku: str, key: str, payload: Dict[str, Any]):
    # força sku/key da URL como fonte de verdade
    payload = dict(payload)
    payload["sku"] = sku
    payload["key"] = key

    try:
        data = normalize_row(payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    pid = make_id(sku, key)

    conn = get_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO prices (id, data) VALUES (?, ?)",
            (pid, json.dumps(data, ensure_ascii=False))
        )
        conn.commit()
        return {"status": "ok", "id": pid, "price": data}
    finally:
        conn.close()


@app.post("/prices/reset", dependencies=[Depends(require_basic_auth)])
def reset_prices(items: List[Dict[str, Any]]):
    conn = get_db()
    try:
        conn.execute("DELETE FROM prices")
        for item in items:
            data = normalize_row(item)
            pid = make_id(data["sku"], data["key"])
            conn.execute(
                "INSERT OR REPLACE INTO prices (id, data) VALUES (?, ?)",
                (pid, json.dumps(data, ensure_ascii=False))
            )
        conn.commit()
        return {"status": "ok", "count": len(items)}
    finally:
        conn.close()
