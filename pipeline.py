"""
Unified CLI & Daemon entry point for MayaCardio Edge Pipeline.

Assessment Section 4 commands:
- pipeline status:
    JSON line: {"mode": "...", "samples_saved": N, "samples_in_cloud": N, "backlog": N}
- pipeline export --session ID --from device (or --from cloud):
    JSON lines: one per sample, then one line per loss
- pipeline results --session ID:
    JSON line per window and mode with Section 3 metrics
- pipeline run:
    Daemon running dual ingestion, network manager, and processing
- pipeline process --session ID:
    Compute window metrics for all windows in a session
"""

import argparse
import json
import os
import sys
import time
import requests
from database import get_db_connection, init_db
from analytics import compute_window_metrics


def cmd_status():
    """Prints current pipeline status as a single JSON line."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM raw_samples")
    total_saved = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM raw_samples WHERE synced = 1")
    total_cloud = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM raw_samples WHERE synced = 0")
    backlog = cursor.fetchone()[0]

    conn.close()

    # Determine mode dynamically by pinging cloud health endpoint
    cloud_endpoint = os.environ.get("CLOUD_ENDPOINT", "http://127.0.0.1:8000/ingest")
    health_url = cloud_endpoint.replace("/ingest", "/health")
    mode = "offline"
    try:
        r = requests.get(health_url, timeout=1.0)
        if r.status_code == 200:
            mode = "online"
    except Exception:
        mode = "offline"

    status_obj = {
        "mode": mode,
        "samples_saved": total_saved,
        "samples_in_cloud": total_cloud,
        "backlog": backlog
    }
    print(json.dumps(status_obj))


def cmd_export(session_id, source):
    """
    Exports data as JSON lines:
    1) One per sample: {"sensor": 1, "index": N, "values": [ch1, ch2, ...]}
    2) One per loss: {"sensor": 2, "from_index": N, "to_index": N, "reason": "..."}
    """
    if source == "cloud":
        cloud_endpoint = os.environ.get("CLOUD_ENDPOINT", "http://127.0.0.1:8000/ingest")
        export_url = cloud_endpoint.replace("/ingest", f"/export?session={session_id}")
        try:
            r = requests.get(export_url, timeout=10.0)
            if r.status_code == 200:
                print(r.text.strip())
                return
        except Exception as e:
            sys.stderr.write(f"Error fetching cloud export: {e}\n")

    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. Export raw samples ordered by sensor, index
    cursor.execute("""
        SELECT sensor, sample_index, ch1, ch2, ch3
        FROM raw_samples
        WHERE session_id = ?
        ORDER BY sensor, sample_index ASC
    """, (session_id,))

    for row in cursor.fetchall():
        vals = [row["ch1"], row["ch2"]]
        if row["ch3"] is not None:
            vals.append(row["ch3"])
        line_obj = {
            "sensor": row["sensor"],
            "index": row["sample_index"],
            "values": vals
        }
        print(json.dumps(line_obj))

    # 2. Export loss log
    cursor.execute("""
        SELECT sensor, from_index, to_index, reason
        FROM loss_log
        WHERE session_id = ?
        ORDER BY sensor, from_index ASC
    """, (session_id,))

    for row in cursor.fetchall():
        loss_obj = {
            "sensor": row["sensor"],
            "from_index": row["from_index"],
            "to_index": row["to_index"],
            "reason": row["reason"]
        }
        print(json.dumps(loss_obj))

    conn.close()


def cmd_results(session_id):
    """Prints Section 3 outputs: one JSON line per window and mode."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM window_results
        WHERE session_id = ?
        ORDER BY window_id, mode ASC
    """, (session_id,))
    rows = cursor.fetchall()

    for row in rows:
        res_obj = {
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
        }
        print(json.dumps(res_obj))

    conn.close()


def cmd_process(session_id, mode="offline"):
    """Compute metrics for all windows of a session."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MIN(sample_index), MAX(sample_index) FROM raw_samples WHERE session_id = ?", (session_id,))
    row = cursor.fetchone()
    conn.close()

    if not row or row[0] is None:
        print(f"No samples found for session {session_id}")
        return

    min_idx, max_idx = row[0], row[1]
    start_window = min_idx // 400
    end_window = max_idx // 400

    print(f"[*] Processing session {session_id}: windows {start_window} to {end_window} ({mode} mode)")
    for w in range(start_window, end_window + 1):
        compute_window_metrics(session_id, w, mode_str=mode)
    print(f"[+] Finished processing {end_window - start_window + 1} windows.")


def cmd_run(host="127.0.0.1", port=7100):
    """Run full edge daemon: Ingestion + Network Sync + Offline Processor."""
    from ingestion import start_ingestion_threads
    from sync import NetworkManager

    print(f"[*] Starting MayaCardio Edge Pipeline Daemon...")
    init_db()

    # Start ingestion threads
    t1, t2 = start_ingestion_threads(host=host, port=port)

    # Start sync manager thread
    nm = NetworkManager()
    sync_thread = threading.Thread(target=nm.run_sync_loop, daemon=True, name="SyncManager")
    sync_thread.start()

    print("[+] Pipeline running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[*] Shutting down pipeline...")
        t1.stop()
        t2.stop()
        nm.stop()
        t1.join()
        t2.join()
        print("[+] Pipeline stopped cleanly.")


if __name__ == "__main__":
    init_db()
    parser = argparse.ArgumentParser(description="MayaCardio Edge Pipeline Controller")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("status")

    export_parser = subparsers.add_parser("export")
    export_parser.add_argument("--session", required=True)
    export_parser.add_argument("--from", required=True, dest="source", choices=["device", "cloud"])

    results_parser = subparsers.add_parser("results")
    results_parser.add_argument("--session", required=True)

    process_parser = subparsers.add_parser("process")
    process_parser.add_argument("--session", required=True)
    process_parser.add_argument("--mode", default="offline", choices=["offline", "online"])

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--host", default="127.0.0.1")
    run_parser.add_argument("--port", type=int, default=7100)

    args = parser.parse_args()

    if args.command == "status":
        cmd_status()
    elif args.command == "export":
        cmd_export(args.session, args.source)
    elif args.command == "results":
        cmd_results(args.session)
    elif args.command == "process":
        cmd_process(args.session, args.mode)
    elif args.command == "run":
        cmd_run(args.host, args.port)
    else:
        parser.print_help()