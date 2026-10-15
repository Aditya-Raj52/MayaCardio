"""
Database module for MayaCardio Edge Pipeline.
Uses SQLite with WAL + synchronous=FULL for crash/power-cut safety.
"""

import sqlite3
import os

DB_FILE = os.environ.get("PIPELINE_DB", "pipeline_storage.db")


def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL mode for concurrent readers + crash safety
    conn.execute("PRAGMA journal_mode=WAL;")
    # FULL sync: every commit is flushed to disk before returning
    conn.execute("PRAGMA synchronous=FULL;")
    return conn


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Raw sensor samples — composite PK prevents duplicates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_samples (
            session_id TEXT,
            sensor     INTEGER,
            sample_index INTEGER,
            ch1  REAL,
            ch2  REAL,
            ch3  REAL,
            synced INTEGER DEFAULT 0,
            PRIMARY KEY (session_id, sensor, sample_index)
        );
    """)

    # Index for fast sync lookup of unsynced records
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_samples_synced
        ON raw_samples (synced, session_id, sample_index);
    """)

    # Loss audit log (criterion 2)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loss_log (
            session_id TEXT,
            sensor     INTEGER,
            from_index INTEGER,
            to_index   INTEGER,
            reason     TEXT
        );
    """)

    # Per-window processing results (section 3)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS window_results (
            session_id      TEXT,
            window_id       INTEGER,
            sensor          INTEGER DEFAULT 1,
            mode            TEXT,
            missing_samples INTEGER DEFAULT 0,
            missing_1       INTEGER DEFAULT 0,
            missing_2       INTEGER DEFAULT 0,
            complete        BOOLEAN,
            s1_ch1_mean     REAL, s1_ch1_min REAL, s1_ch1_max REAL,
            s1_ch2_mean     REAL, s1_ch2_min REAL, s1_ch2_max REAL,
            s1_ch3_mean     REAL, s1_ch3_min REAL, s1_ch3_max REAL,
            s2_ch1_mean     REAL, s2_ch1_min REAL, s2_ch1_max REAL,
            s2_ch2_mean     REAL, s2_ch2_min REAL, s2_ch2_max REAL,
            mean_val        REAL, min_val    REAL, max_val    REAL,
            magnitude       REAL,
            heart_rate      REAL,
            PRIMARY KEY (session_id, window_id, mode)
        );
    """)

    # Track the latest session_id seen per sensor
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS session_state (
            sensor       INTEGER PRIMARY KEY,
            session_id   TEXT,
            last_index   INTEGER DEFAULT -1
        );
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully with WAL safety.")