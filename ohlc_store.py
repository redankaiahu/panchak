# =============================================================
# ohlc_store.py  — Local SQLite OHLC Database for Hourly Candles
# =============================================================
# Creates and maintains a local SQLite database:
#     ohlc_1h.db  (in same folder as dashboard)
#
# Tables:
#     candles_1h  — hourly OHLC for all stocks
#     meta        — last update timestamps per symbol
#
# Usage:
#     from ohlc_store import OHLCStore
#     db = OHLCStore()
#     db.update(kite, symbols)          # fetch + store new candles
#     candles = db.get(symbol, n=30)    # get last 30 hourly candles
# =============================================================

from __future__ import annotations
import sqlite3
import os
import time
from datetime import datetime, date, timedelta
from typing import Optional

try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
except ImportError:
    IST = None

DB_FILE      = "ohlc_1h.db"
HISTORY_DAYS = 60          # how many calendar days to fetch on first run
MAX_CANDLES  = 200         # max stored per symbol (keeps DB small)

# NSE market hours
MARKET_OPEN  = 9 * 60 + 15   # 09:15 in minutes
MARKET_CLOSE = 15 * 60 + 30  # 15:30 in minutes


# ─────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────

CREATE_CANDLES = """
CREATE TABLE IF NOT EXISTS candles_1h (
    symbol      TEXT    NOT NULL,
    dt          TEXT    NOT NULL,   -- ISO datetime string "2026-04-08 10:15:00"
    open        REAL    NOT NULL,
    high        REAL    NOT NULL,
    low         REAL    NOT NULL,
    close       REAL    NOT NULL,
    volume      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, dt)
)
"""

CREATE_META = """
CREATE TABLE IF NOT EXISTS meta (
    symbol          TEXT PRIMARY KEY,
    last_updated    TEXT,           -- ISO datetime of last successful fetch
    last_candle_dt  TEXT,           -- dt of most recent candle stored
    total_candles   INTEGER DEFAULT 0
)
"""

CREATE_IDX = """
CREATE INDEX IF NOT EXISTS idx_candles_symbol_dt
    ON candles_1h (symbol, dt DESC)
"""


# ─────────────────────────────────────────────────────────────
# OHLC STORE CLASS
# ─────────────────────────────────────────────────────────────

class OHLCStore:
    """
    Local SQLite store for 1-hour OHLC data.
    Thread-safe via connection-per-call pattern.
    """

    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")   # concurrent reads
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        with self._conn() as conn:
            conn.execute(CREATE_CANDLES)
            conn.execute(CREATE_META)
            conn.execute(CREATE_IDX)
            conn.commit()

    # ── READ ─────────────────────────────────────────────────

    def get(self, symbol: str, n: int = 50) -> list[dict]:
        """
        Get the last N hourly candles for a symbol.
        Returns list of dicts sorted oldest → newest.
        """
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT dt, open, high, low, close, volume
                FROM   candles_1h
                WHERE  symbol = ?
                ORDER  BY dt DESC
                LIMIT  ?
                """,
                (symbol, n),
            ).fetchall()

        return [
            {
                "datetime": r["dt"],
                "open":     r["open"],
                "high":     r["high"],
                "low":      r["low"],
                "close":    r["close"],
                "volume":   r["volume"],
                "symbol":   symbol,
            }
            for r in reversed(rows)   # flip to oldest-first
        ]

    def get_last_candle_dt(self, symbol: str) -> Optional[str]:
        """Get the datetime string of the most recent stored candle."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT last_candle_dt FROM meta WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row["last_candle_dt"] if row else None

    def get_all_symbols(self) -> list[str]:
        """List all symbols that have data in the store."""
        with self._conn() as conn:
            rows = conn.execute("SELECT DISTINCT symbol FROM meta").fetchall()
        return [r["symbol"] for r in rows]

    def get_status(self) -> list[dict]:
        """Get update status for all symbols — for dashboard display."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT symbol, last_updated, last_candle_dt, total_candles
                FROM   meta
                ORDER  BY symbol
                """
            ).fetchall()
        return [dict(r) for r in rows]

    def get_db_size_mb(self) -> float:
        if os.path.exists(self.db_path):
            return round(os.path.getsize(self.db_path) / 1024 / 1024, 2)
        return 0.0

    # ── WRITE ────────────────────────────────────────────────

    def upsert_candles(self, symbol: str, candles: list[dict]):
        """
        Insert or update candles for a symbol.
        Keeps only the last MAX_CANDLES rows per symbol to control DB size.
        """
        if not candles:
            return 0

        rows = []
        for c in candles:
            dt_str = _normalize_dt(c.get("datetime") or c.get("date", ""))
            if not dt_str:
                continue
            rows.append((
                symbol,
                dt_str,
                float(c.get("open",  0)),
                float(c.get("high",  0)),
                float(c.get("low",   0)),
                float(c.get("close", 0)),
                int(c.get("volume",  0)),
            ))

        if not rows:
            return 0

        with self._conn() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO candles_1h
                    (symbol, dt, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

            # Prune old candles beyond MAX_CANDLES
            conn.execute(
                """
                DELETE FROM candles_1h
                WHERE symbol = ?
                AND   dt NOT IN (
                    SELECT dt FROM candles_1h
                    WHERE  symbol = ?
                    ORDER  BY dt DESC
                    LIMIT  ?
                )
                """,
                (symbol, symbol, MAX_CANDLES),
            )

            # Update meta
            latest_dt = max(r[1] for r in rows)
            total = conn.execute(
                "SELECT COUNT(*) as n FROM candles_1h WHERE symbol = ?",
                (symbol,),
            ).fetchone()["n"]

            conn.execute(
                """
                INSERT OR REPLACE INTO meta
                    (symbol, last_updated, last_candle_dt, total_candles)
                VALUES (?, ?, ?, ?)
                """,
                (
                    symbol,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    latest_dt,
                    total,
                ),
            )
            conn.commit()

        return len(rows)

    # ── FETCH FROM KITE + STORE ───────────────────────────────

    def update_symbol(
        self,
        kite,
        symbol: str,
        token: int,
        days_initial: int = HISTORY_DAYS,
    ) -> dict:
        """
        Fetch new hourly candles from Kite for one symbol and store them.
        On first run: fetches HISTORY_DAYS of history.
        On subsequent runs: fetches only since last stored candle.

        Returns: {symbol, fetched, stored, last_candle_dt, error}
        """
        result = {
            "symbol":        symbol,
            "fetched":       0,
            "stored":        0,
            "last_candle_dt": None,
            "error":         None,
        }

        try:
            last_dt = self.get_last_candle_dt(symbol)

            if last_dt:
                # Incremental: fetch from last candle date
                from_date = datetime.strptime(last_dt[:10], "%Y-%m-%d").date()
                # Go back 1 day to catch any late-arriving candles
                from_date = from_date - timedelta(days=1)
            else:
                # First run: full history
                from_date = date.today() - timedelta(days=days_initial)

            to_date = date.today()

            raw = kite.historical_data(
                instrument_token = token,
                from_date        = from_date,
                to_date          = to_date,
                interval         = "60minute",
                continuous       = False,
                oi               = False,
            )

            result["fetched"] = len(raw)

            if raw:
                stored = self.upsert_candles(symbol, raw)
                result["stored"]        = stored
                result["last_candle_dt"] = self.get_last_candle_dt(symbol)

        except Exception as e:
            result["error"] = str(e)

        return result

    def update_all(
        self,
        kite,
        symbols: list[str],
        get_token_fn,
        batch_size:  int   = 10,
        delay_secs:  float = 0.3,
        log_fn = None,
    ) -> dict:
        """
        Update all symbols. Runs in batches to respect Kite rate limits.

        Parameters:
            kite         : Kite connect instance
            symbols      : List of symbol strings
            get_token_fn : Function(symbol) → instrument_token int
            batch_size   : How many symbols to fetch per batch
            delay_secs   : Delay between batches (rate limit safety)
            log_fn       : Optional logging function (e.g. print or st.write)

        Returns summary dict.
        """
        results   = []
        errors    = []
        updated   = 0
        skipped   = 0

        if log_fn:
            log_fn(f"OHLCStore: updating {len(symbols)} symbols...")

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i: i + batch_size]

            for symbol in batch:
                # Skip indices — no hourly equity data
                if symbol in ("NIFTY","BANKNIFTY","FINNIFTY","NIFTYIT",
                              "NIFTYFMCG","NIFTYPHARMA","NIFTYMETAL",
                              "NIFTYAUTO","NIFTYENERGY","NIFTYPSUBANK"):
                    skipped += 1
                    continue

                token = get_token_fn(symbol)
                if not token:
                    skipped += 1
                    continue

                r = self.update_symbol(kite, symbol, token)
                results.append(r)

                if r["error"]:
                    errors.append(f"{symbol}: {r['error']}")
                else:
                    updated += 1

            # Rate limit delay between batches
            if i + batch_size < len(symbols):
                time.sleep(delay_secs)

        summary = {
            "total":    len(symbols),
            "updated":  updated,
            "skipped":  skipped,
            "errors":   len(errors),
            "error_list": errors[:10],   # first 10 errors
            "db_size_mb": self.get_db_size_mb(),
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }

        if log_fn:
            log_fn(
                f"OHLCStore done: {updated} updated, {skipped} skipped, "
                f"{len(errors)} errors | DB: {summary['db_size_mb']} MB"
            )

        return summary

    def is_update_needed(self, max_age_minutes: int = 65) -> bool:
        """
        Returns True if any symbol hasn't been updated in max_age_minutes.
        Used to decide whether to run a full update on dashboard refresh.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT MIN(last_updated) as oldest FROM meta"
            ).fetchone()

        if not row or not row["oldest"]:
            return True

        try:
            oldest = datetime.strptime(row["oldest"], "%Y-%m-%d %H:%M:%S")
            age    = (datetime.now() - oldest).total_seconds() / 60
            return age > max_age_minutes
        except Exception:
            return True


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _normalize_dt(dt_raw) -> str:
    """
    Convert various datetime formats to a consistent string:
    "YYYY-MM-DD HH:MM:SS"
    """
    if not dt_raw:
        return ""
    try:
        if isinstance(dt_raw, datetime):
            return dt_raw.strftime("%Y-%m-%d %H:%M:%S")
        s = str(dt_raw)
        # Handle "+05:30" timezone suffix
        s = s.replace("+05:30", "").replace("T", " ").strip()
        # Handle milliseconds
        if "." in s:
            s = s[:s.index(".")]
        # Validate by parsing
        datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        return s[:19]
    except Exception:
        return str(dt_raw)[:19]


def is_market_hours() -> bool:
    """True if NSE market is currently open."""
    now = datetime.now(IST) if IST else datetime.now()
    mins = now.hour * 60 + now.minute
    return (
        now.weekday() < 5 and
        MARKET_OPEN <= mins <= MARKET_CLOSE
    )


def next_candle_close_mins() -> int:
    """Minutes until the next hourly candle closes (15, 30, etc.)"""
    now  = datetime.now(IST) if IST else datetime.now()
    mins = now.minute
    return 60 - mins   # minutes until next hour


# ─────────────────────────────────────────────────────────────
# STREAMLIT STATUS PANEL
# ─────────────────────────────────────────────────────────────

def render_db_status(db: OHLCStore):
    """Show database status in Streamlit (for ⚡ Alerts tab or sidebar)."""
    try:
        import streamlit as st
    except ImportError:
        return

    status = db.get_status()
    size   = db.get_db_size_mb()

    st.markdown(f"**📦 OHLC Database** — `{db.db_path}` — {size} MB")

    if not status:
        st.warning("Database is empty. Run an update first.")
        return

    total_syms    = len(status)
    total_candles = sum(s["total_candles"] for s in status)
    updated_today = sum(
        1 for s in status
        if s["last_updated"] and s["last_updated"][:10] == date.today().isoformat()
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Symbols", total_syms)
    c2.metric("Total Candles", f"{total_candles:,}")
    c3.metric("Updated Today", updated_today)

    with st.expander("📋 Per-Symbol Status", expanded=False):
        import pandas as pd
        df = pd.DataFrame(status)
        df = df.rename(columns={
            "symbol": "Symbol",
            "last_updated": "Last Updated",
            "last_candle_dt": "Last Candle",
            "total_candles": "Candles",
        })
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=300,
        )


# ─────────────────────────────────────────────────────────────
# STANDALONE TEST
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("OHLCStore standalone test")
    db = OHLCStore("test_ohlc.db")

    # Insert synthetic candles
    import random
    base = 1280.0
    fake = []
    for i in range(40):
        dt = datetime(2026, 4, 1, 9, 15) + timedelta(hours=i)
        o  = base + i * 2 + random.uniform(-3, 3)
        c  = o + random.uniform(-5, 8)
        fake.append({
            "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": o, "high": max(o, c) + 2,
            "low": min(o, c) - 2, "close": c, "volume": 100000,
        })

    stored = db.upsert_candles("RELIANCE", fake)
    print(f"Stored {stored} candles for RELIANCE")

    candles = db.get("RELIANCE", n=10)
    print(f"Retrieved {len(candles)} candles")
    for c in candles[-3:]:
        print(f"  {c['datetime']}  O:{c['open']:.1f}  H:{c['high']:.1f}  "
              f"L:{c['low']:.1f}  C:{c['close']:.1f}")

    print(f"DB size: {db.get_db_size_mb()} MB")
    print(f"Update needed: {db.is_update_needed()}")

    os.remove("test_ohlc.db")
    print("Test passed.")
