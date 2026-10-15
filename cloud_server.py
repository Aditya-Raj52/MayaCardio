"""
Cloud-side service for MayaCardio Pipeline.

Deployable on any free-tier cloud host (Render, Railway, Fly.io, Koyeb, HuggingFace Spaces).
Endpoints:
- GET  /health: Health check endpoint for edge connection detection
- POST /ingest: Receives gzipped or raw batches of raw samples and processes windows live
- GET  /status: Reports cloud stored sample count and processing stats
- GET  /export: Serves cloud-stored samples and losses in JSON-line format
- GET  /results: Serves processed window results in JSON-line format
"""

import gzip
import json
import math
import os
import sqlite3
import sys
from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse

app = FastAPI(title="MayaCardio Cloud Ingestion Service")

CLOUD_DB = os.environ.get("CLOUD_DB", "cloud_storage.db")


def get_cloud_db():
    conn = sqlite3.connect(CLOUD_DB, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_cloud_db():
    conn = get_cloud_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS cloud_samples (
            session_id TEXT,
            sensor     INTEGER,
            sample_index INTEGER,
            ch1  REAL,
            ch2  REAL,
            ch3  REAL,
            PRIMARY KEY (session_id, sensor, sample_index)
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cloud_loss_log (
            session_id TEXT,
            sensor     INTEGER,
            from_index INTEGER,
            to_index   INTEGER,
            reason     TEXT
        );
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cloud_results (
            session_id  TEXT,
            window_id   INTEGER,
            mode        TEXT DEFAULT 'online',
            missing_1   INTEGER,
            missing_2   INTEGER,
            complete    BOOLEAN,
            s1_ch1_mean REAL, s1_ch1_min REAL, s1_ch1_max REAL,
            s1_ch2_mean REAL, s1_ch2_min REAL, s1_ch2_max REAL,
            s1_ch3_mean REAL, s1_ch3_min REAL, s1_ch3_max REAL,
            s2_ch1_mean REAL, s2_ch1_min REAL, s2_ch1_max REAL,
            s2_ch2_mean REAL, s2_ch2_min REAL, s2_ch2_max REAL,
            magnitude   REAL,
            heart_rate  REAL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (session_id, window_id, mode)
        );
    """)
    conn.commit()
    conn.close()


init_cloud_db()


def _stats(values):
    if not values:
        return None, None, None
    return round(sum(values) / len(values), 1), round(min(values), 1), round(max(values), 1)


def process_cloud_window(conn, session_id, window_w):
    """Process online window w according to Section 3."""
    cursor = conn.cursor()
    start_sample = 400 * window_w
    end_sample = start_sample + 399
    expected_indices = set(range(start_sample, end_sample + 1))

    # Sensor 1
    cursor.execute("""
        SELECT sample_index, ch1, ch2, ch3 FROM cloud_samples
        WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?
        ORDER BY sample_index ASC
    """, (session_id, start_sample, end_sample))
    s1_rows = cursor.fetchall()
    missing_1 = len(expected_indices - {r["sample_index"] for r in s1_rows})

    # Sensor 2
    cursor.execute("""
        SELECT sample_index, ch1, ch2 FROM cloud_samples
        WHERE session_id = ? AND sensor = 2 AND sample_index BETWEEN ? AND ?
        ORDER BY sample_index ASC
    """, (session_id, start_sample, end_sample))
    s2_rows = cursor.fetchall()
    missing_2 = len(expected_indices - {r["sample_index"] for r in s2_rows})

    complete = (missing_1 == 0 and missing_2 == 0)

    s1_ch1_mean, s1_ch1_min, s1_ch1_max = _stats([r["ch1"] for r in s1_rows if r["ch1"] is not None])
    s1_ch2_mean, s1_ch2_min, s1_ch2_max = _stats([r["ch2"] for r in s1_rows if r["ch2"] is not None])
    s1_ch3_mean, s1_ch3_min, s1_ch3_max = _stats([r["ch3"] for r in s1_rows if r["ch3"] is not None])

    s2_ch1_mean, s2_ch1_min, s2_ch1_max = _stats([r["ch1"] for r in s2_rows if r["ch1"] is not None])
    s2_ch2_mean, s2_ch2_min, s2_ch2_max = _stats([r["ch2"] for r in s2_rows if r["ch2"] is not None])

    s2_mags = [math.sqrt(r["ch1"]**2 + r["ch2"]**2) for r in s2_rows if r["ch1"] is not None and r["ch2"] is not None]
    magnitude = round(sum(s2_mags) / len(s2_mags), 1) if s2_mags else None

    # Heart rate over last 5 seconds (Sensor 1, Channel 2)
    heart_rate = None
    if window_w >= 4:
        five_sec_start = 400 * (window_w - 4)
        five_sec_end = 400 * (window_w + 1) - 1
        five_sec_total = five_sec_end - five_sec_start + 1

        cursor.execute("""
            SELECT sample_index, ch2 FROM cloud_samples
            WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?
            ORDER BY sample_index ASC
        """, (session_id, five_sec_start, five_sec_end))
        hr_rows = cursor.fetchall()

        if len(hr_rows) == five_sec_total:
            ch2_data = [r["ch2"] for r in hr_rows]
            peaks = []
            for i in range(800, len(ch2_data) - 1):
                val = ch2_data[i]
                prev_val = ch2_data[i - 1]
                next_val = ch2_data[i + 1]
                if val <= prev_val or val <= next_val:
                    continue
                past_800 = ch2_data[i - 800 : i]
                if val <= (0.5 * max(past_800)):
                    continue
                if not peaks or (i - peaks[-1]) >= 100:
                    peaks.append(i)

            if len(peaks) >= 2:
                gaps = [peaks[j] - peaks[j - 1] for j in range(1, len(peaks))]
                mean_gap_seconds = (sum(gaps) / len(gaps)) / 400.0
                if mean_gap_seconds > 0:
                    heart_rate = round(60.0 / mean_gap_seconds, 1)

    cursor.execute("""
        INSERT OR REPLACE INTO cloud_results (
            session_id, window_id, mode, missing_1, missing_2, complete,
            s1_ch1_mean, s1_ch1_min, s1_ch1_max,
            s1_ch2_mean, s1_ch2_min, s1_ch2_max,
            s1_ch3_mean, s1_ch3_min, s1_ch3_max,
            s2_ch1_mean, s2_ch1_min, s2_ch1_max,
            s2_ch2_mean, s2_ch2_min, s2_ch2_max,
            magnitude, heart_rate
        ) VALUES (?, ?, 'online', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        session_id, window_w, missing_1, missing_2, 1 if complete else 0,
        s1_ch1_mean, s1_ch1_min, s1_ch1_max,
        s1_ch2_mean, s1_ch2_min, s1_ch2_max,
        s1_ch3_mean, s1_ch3_min, s1_ch3_max,
        s2_ch1_mean, s2_ch1_min, s2_ch1_max,
        s2_ch2_mean, s2_ch2_min, s2_ch2_max,
        magnitude, heart_rate
    ))


@app.get("/health")
def health():
    return {"status": "ok", "service": "mayacardio-cloud"}


@app.post("/ingest")
async def ingest(request: Request):
    content_encoding = request.headers.get("content-encoding", "")
    raw_body = await request.body()

    if "gzip" in content_encoding:
        payload_text = gzip.decompress(raw_body).decode('utf-8')
    else:
        payload_text = raw_body.decode('utf-8')

    data = json.loads(payload_text)
    samples = data.get("samples", [])
    losses = data.get("losses", [])

    conn = get_cloud_db()
    cursor = conn.cursor()

    # Insert raw samples
    sample_rows = []
    affected_windows = set()

    for s in samples:
        session_id = s["session_id"]
        sensor = s["sensor"]
        idx = s["index"]
        vals = s["values"]
        ch1 = vals[0] if len(vals) > 0 else None
        ch2 = vals[1] if len(vals) > 1 else None
        ch3 = vals[2] if len(vals) > 2 else None
        sample_rows.append((session_id, sensor, idx, ch1, ch2, ch3))
        affected_windows.add((session_id, idx // 400))

    if sample_rows:
        cursor.executemany("""
            INSERT OR IGNORE INTO cloud_samples (session_id, sensor, sample_index, ch1, ch2, ch3)
            VALUES (?, ?, ?, ?, ?, ?)
        """, sample_rows)

    # Insert loss logs
    for l in losses:
        cursor.execute("""
            INSERT INTO cloud_loss_log (session_id, sensor, from_index, to_index, reason)
            VALUES (?, ?, ?, ?, ?)
        """, (l["session_id"], l["sensor"], l["from_index"], l.get("to_index"), l.get("reason", "unknown")))

    conn.commit()

    # Online processing for affected windows
    for sess_id, win_id in affected_windows:
        process_cloud_window(conn, sess_id, win_id)

    conn.commit()
    conn.close()

    return {"status": "accepted", "inserted": len(sample_rows)}


@app.get("/status")
def status():
    conn = get_cloud_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM cloud_samples")
    total_samples = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM cloud_results")
    total_results = c.fetchone()[0]
    conn.close()
    return {
        "status": "online",
        "cloud_samples": total_samples,
        "windows_processed": total_results
    }


@app.get("/export", response_class=PlainTextResponse)
def export(session: str):
    conn = get_cloud_db()
    c = conn.cursor()
    lines = []

    # Samples
    c.execute("""
        SELECT sensor, sample_index, ch1, ch2, ch3
        FROM cloud_samples
        WHERE session_id = ?
        ORDER BY sensor, sample_index ASC
    """, (session,))
    for r in c.fetchall():
        vals = [r["ch1"], r["ch2"]]
        if r["ch3"] is not None:
            vals.append(r["ch3"])
        lines.append(json.dumps({"sensor": r["sensor"], "index": r["sample_index"], "values": vals}))

    # Losses
    c.execute("""
        SELECT sensor, from_index, to_index, reason
        FROM cloud_loss_log
        WHERE session_id = ?
        ORDER BY sensor, from_index ASC
    """, (session,))
    for r in c.fetchall():
        lines.append(json.dumps({
            "sensor": r["sensor"],
            "from_index": r["from_index"],
            "to_index": r["to_index"],
            "reason": r["reason"]
        }))

    conn.close()
    return "\n".join(lines)


@app.get("/results", response_class=PlainTextResponse)
def results(session: str):
    conn = get_cloud_db()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM cloud_results WHERE session_id = ? ORDER BY window_id ASC
    """, (session,))
    lines = []
    for row in c.fetchall():
        lines.append(json.dumps({
            "session_id": row["session_id"],
            "window_id": row["window_id"],
            "mode": row["mode"],
            "missing_1": row["missing_1"],
            "missing_2": row["missing_2"],
            "complete": bool(row["complete"]),
            "s1_ch1": {"mean": row["s1_ch1_mean"], "min": row["s1_ch1_min"], "max": row["s1_ch1_max"]},
            "s1_ch2": {"mean": row["s1_ch2_mean"], "min": row["s1_ch2_min"], "max": row["s1_ch2_max"]},
            "s1_ch3": {"mean": row["s1_ch3_mean"], "min": row["s1_ch3_min"], "max": row["s1_ch3_max"]},
            "s2_ch1": {"mean": row["s2_ch1_mean"], "min": row["s2_ch1_min"], "max": row["s2_ch1_max"]},
            "s2_ch2": {"mean": row["s2_ch2_mean"], "min": row["s2_ch2_min"], "max": row["s2_ch2_max"]},
            "magnitude": row["magnitude"],
            "heart_rate": row["heart_rate"]
        }))
    conn.close()
    return "\n".join(lines)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    print(f"[*] Starting MayaCardio Cloud Service on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
