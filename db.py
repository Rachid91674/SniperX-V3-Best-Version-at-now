import sqlite3
import json
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "sniperx_tracker.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS tokens_stage_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                address TEXT,
                name TEXT,
                stage TEXT,
                passed INTEGER,
                details_json TEXT
        )""")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS risk_analysis (
                address TEXT PRIMARY KEY,
                liquidity_usd REAL,
                volume_usd REAL,
                price_impact_percent REAL,
                cluster_dominance_percent REAL,
                dump_risk_ratio REAL,
                risk_flag TEXT,
                source TEXT
        )""")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS trades (
                trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT,
                buy_price REAL,
                sell_price REAL,
                pnl_percent REAL,
                timestamp_buy TEXT,
                timestamp_sell TEXT
        )""")
    conn.commit()
    conn.close()

init_db()

def _execute(query, params):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    conn.close()


def log_stage_token(address: str, name: str, stage: str, passed: bool, metrics_dict: dict | None = None):
    metrics_json = json.dumps(metrics_dict or {})
    _execute(
        "INSERT INTO tokens_stage_log(timestamp, address, name, stage, passed, details_json) "
        "VALUES(?,?,?,?,?,?)",
        (datetime.utcnow().isoformat(), address, name, stage, int(bool(passed)), metrics_json)
    )
    print(f"[DB] Logged stage '{stage}' for {name} ({address})")


def log_risk_analysis(address: str, liquidity_usd: float | None, volume_usd: float | None,
                      price_impact_percent: float | None, cluster_dominance_percent: float | None,
                      dump_risk_ratio: float | None, risk_flag: str, source: str):
    _execute(
        "INSERT OR REPLACE INTO risk_analysis(address, liquidity_usd, volume_usd, "
        "price_impact_percent, cluster_dominance_percent, dump_risk_ratio, risk_flag, source) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (
            address,
            liquidity_usd,
            volume_usd,
            price_impact_percent,
            cluster_dominance_percent,
            dump_risk_ratio,
            risk_flag,
            source,
        ),
    )
    print(f"[DB] Logged risk analysis for {address}")


def log_trade(address: str, buy_price: float | None, sell_price: float | None,
              timestamp_buy: str | None, timestamp_sell: str | None):
    pnl = None
    if buy_price is not None and sell_price is not None and buy_price != 0:
        pnl = ((sell_price - buy_price) / buy_price) * 100.0
    _execute(
        "INSERT INTO trades(token_address, buy_price, sell_price, pnl_percent, timestamp_buy, timestamp_sell) "
        "VALUES(?,?,?,?,?,?)",
        (address, buy_price, sell_price, pnl, timestamp_buy, timestamp_sell),
    )
    print(f"[DB] Logged trade for {address}")
