"""
NAS Signal Tester - Database Layer
SQLite database for tracking signals, positions, prices, and scores.
"""
import sqlite3
import os
from datetime import datetime, timezone

from config import DB_PATH, DATA_DIR


def get_conn():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""

    -- Received signal_packs with full metadata
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id TEXT UNIQUE NOT NULL,
        rss_guid TEXT UNIQUE,
        title TEXT,
        report_url TEXT,
        published_date TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),

        primary_asset TEXT,
        direction_bias TEXT,
        pressure_index REAL,
        acceleration_delta REAL,
        signal_strength TEXT,
        decay_window_hours REAL,
        top_driver_clusters TEXT,

        signal_pack_json TEXT,

        cascade_count INTEGER DEFAULT 0,
        position_count INTEGER DEFAULT 0
    );

    -- Paper positions from cascade map entries
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id TEXT NOT NULL,
        cascade_category TEXT NOT NULL,
        expected_direction TEXT NOT NULL,
        proxy_ticker TEXT,
        resolve_method TEXT,

        state TEXT DEFAULT 'ACTIVE',
        state_reason TEXT,
        opened_at TEXT NOT NULL,
        closed_at TEXT,
        close_reason TEXT,

        reinforced_count INTEGER DEFAULT 0,
        reinforced_by TEXT,
        last_reinforced_at TEXT,

        decay_window_hours REAL,
        monitoring_until TEXT,

        entry_price REAL,
        entry_price_source TEXT,
        entry_price_time TEXT,

        trigger_condition TEXT,
        invalidation_condition TEXT,

        hit_6h INTEGER,
        hit_12h INTEGER,
        hit_24h INTEGER,
        hit_72h INTEGER,
        earliest_hit_hours REAL,
        max_favorable_excursion REAL,
        max_adverse_excursion REAL,

        t_belief TEXT,
        t_reality TEXT,
        t_peak TEXT,
        lead_time_hours REAL,
        advantage_state TEXT DEFAULT 'AHEAD',

        is_active INTEGER DEFAULT 1,

        FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
    );

    -- Price at each sample time
    CREATE TABLE IF NOT EXISTS price_samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        position_id INTEGER NOT NULL,
        ticker TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        price REAL,
        open_price REAL,
        high REAL,
        low REAL,
        volume REAL,
        change_pct REAL,

        hours_since_open REAL,
        return_pct REAL,
        is_horizon_sample INTEGER DEFAULT 0,

        source TEXT DEFAULT 'yfinance',

        FOREIGN KEY (position_id) REFERENCES positions(id)
    );

    -- Computed scores per position per horizon
    CREATE TABLE IF NOT EXISTS position_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        position_id INTEGER NOT NULL,
        horizon_hours REAL NOT NULL,
        scored_at TEXT NOT NULL,

        return_pct REAL,
        expected_direction TEXT,
        min_move_pct REAL,
        passed INTEGER,

        sample_price REAL,
        entry_price REAL,

        FOREIGN KEY (position_id) REFERENCES positions(id)
    );

    -- Cached instrument resolutions
    CREATE TABLE IF NOT EXISTS resolver_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        exposure_category TEXT UNIQUE NOT NULL,
        proxy_ticker TEXT NOT NULL,
        resolve_method TEXT,
        resolved_at TEXT DEFAULT (datetime('now')),
        confidence TEXT DEFAULT 'high',
        notes TEXT
    );

    -- Aggregated daily stats for charting
    CREATE TABLE IF NOT EXISTS daily_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE NOT NULL,
        signals_received INTEGER DEFAULT 0,
        positions_opened INTEGER DEFAULT 0,
        positions_closed INTEGER DEFAULT 0,
        active_positions INTEGER DEFAULT 0,

        hit_rate_6h REAL,
        hit_rate_12h REAL,
        hit_rate_24h REAL,
        hit_rate_72h REAL,
        avg_lead_time_hours REAL,
        avg_advantage_window_hours REAL,

        high_strength_hit_rate REAL,
        moderate_strength_hit_rate REAL
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_positions_signal
        ON positions(signal_id);
    CREATE INDEX IF NOT EXISTS idx_positions_active
        ON positions(is_active, state);
    CREATE INDEX IF NOT EXISTS idx_positions_ticker
        ON positions(proxy_ticker);
    CREATE INDEX IF NOT EXISTS idx_samples_position
        ON price_samples(position_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_scores_position
        ON position_scores(position_id, horizon_hours);
    CREATE INDEX IF NOT EXISTS idx_daily_date
        ON daily_summary(date);
    CREATE INDEX IF NOT EXISTS idx_resolver_category
        ON resolver_cache(exposure_category);

    """)

    # Safe migration: add unique constraint on position_scores if needed
    # (handled by UNIQUE index below)
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_scores_unique
            ON position_scores(position_id, horizon_hours)
        """)
    except Exception:
        pass

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database initialised at {}".format(DB_PATH))
