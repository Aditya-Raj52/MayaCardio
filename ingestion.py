"""
Ingestion module for MayaCardio Edge Pipeline.

Reads both sensors continuously from the mock service over TCP:
- Sensor 1 (3 channels, 400 Hz)
- Sensor 2 (2 channels, 400 Hz)
- Validates CRC-32 over the 'data' array text
- Re-reads data via 'from_index' upon CRC mismatch
- Tracks session changes and audit-logs data loss
- Persists all samples directly to SQLite with WAL mode
"""

import json
import os
import socket
import sys
import threading
import time
import zlib
from database import get_db_connection, init_db


def extract_data_array_bytes(raw_line_bytes):
    """
    Extract the exact JSON byte substring corresponding to the 'data' array,
    from '[' to the matching ']'.
    This avoids picking up brackets from other fields like 'lost': [].
    """
    data_idx = raw_line_bytes.find(b'"data"')
    if data_idx == -1:
        return None
    start_bracket = raw_line_bytes.find(b'[', data_idx)
    if start_bracket == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    for i in range(start_bracket, len(raw_line_bytes)):
        b = raw_line_bytes[i:i+1]
        if escape:
            escape = False
            continue
        if b == b'\\':
            escape = True
            continue
        if b == b'"':
            in_string = not in_string
            continue
        if not in_string:
            if b == b'[':
                depth += 1
            elif b == b']':
                depth -= 1
                if depth == 0:
                    return raw_line_bytes[start_bracket:i+1]
    return None


def verify_crc32(raw_line_bytes):
    """
    Extract data array bytes and compute CRC32 formatted as lowercase 8-digit hex.
    Returns 8-character hex string or None.
    """
    payload = extract_data_array_bytes(raw_line_bytes)
    if payload is None:
        return None
    return format(zlib.crc32(payload) & 0xFFFFFFFF, '08x')


class SensorPoller(threading.Thread):
    """Worker thread dedicated to polling a single sensor continuously."""

    def __init__(self, sensor_id, host='127.0.0.1', port=7100, poll_interval=0.05):
        super().__init__(daemon=True, name=f"Poller-Sensor-{sensor_id}")
        self.sensor_id = sensor_id
        self.host = host
        self.port = port
        self.poll_interval = poll_interval
        self.running = True
        self.last_index = -1
        self.last_session_id = None
        self.sock = None

    def _connect(self):
        while self.running:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5.0)
                s.connect((self.host, self.port))
                self.sock = s
                return True
            except (ConnectionRefusedError, socket.timeout, OSError):
                time.sleep(1.0)
        return False

    def run(self):
        conn = get_db_connection()
        buffer = b""

        while self.running:
            if self.sock is None:
                if not self._connect():
                    break

            try:
                # Request next data
                cmd_dict = {"cmd": "read", "sensor": self.sensor_id}
                cmd_bytes = json.dumps(cmd_dict).encode('utf-8') + b'\n'
                self.sock.sendall(cmd_bytes)

                # Read line
                while b'\n' not in buffer:
                    chunk = self.sock.recv(65536)
                    if not chunk:
                        raise ConnectionResetError("Connection closed by peer")
                    buffer += chunk

                line, buffer = buffer.split(b'\n', 1)
                line = line.strip()
                if not line:
                    continue

                # Verify CRC32
                computed_crc = verify_crc32(line)
                data_dict = json.loads(line.decode('utf-8'))
                expected_crc = data_dict.get("crc32", "")

                if computed_crc is None or computed_crc.lower().zfill(8) != expected_crc.lower().zfill(8):
                    print(f"[!] Sensor {self.sensor_id} CRC mismatch (exp={expected_crc}, got={computed_crc}). Re-reading...")
                    # Re-read using from_index if available
                    if self.last_index >= 0:
                        retry_cmd = json.dumps({"cmd": "read", "sensor": self.sensor_id, "from_index": self.last_index + 1}) + "\n"
                        self.sock.sendall(retry_cmd.encode('utf-8'))
                    continue

                session_id = data_dict.get("session_id")
                first_index = data_dict.get("first_index", 0)
                samples = data_dict.get("data", [])
                lost_ranges = data_dict.get("lost", [])
                session_end = data_dict.get("session_end", False)

                cursor = conn.cursor()

                # Detect session restart (mock server restarted)
                if self.last_session_id is not None and session_id != self.last_session_id:
                    # Previous session unread data is lost
                    cursor.execute("""
                        INSERT INTO loss_log (session_id, sensor, from_index, to_index, reason)
                        VALUES (?, ?, ?, NULL, 'session_restart')
                    """, (self.last_session_id, self.sensor_id, self.last_index + 1))
                    self.last_index = -1

                self.last_session_id = session_id

                # Record reported lost ranges
                for loss in lost_ranges:
                    cursor.execute("""
                        INSERT INTO loss_log (session_id, sensor, from_index, to_index, reason)
                        VALUES (?, ?, ?, ?, ?)
                    """, (session_id, self.sensor_id, loss.get("from_index"), loss.get("to_index"), loss.get("reason", "unknown")))

                # Check for gap between last_index and first_index
                if self.last_index >= 0 and first_index > (self.last_index + 1):
                    cursor.execute("""
                        INSERT INTO loss_log (session_id, sensor, from_index, to_index, reason)
                        VALUES (?, ?, ?, ?, 'buffer_gap')
                    """, (session_id, self.sensor_id, self.last_index + 1, first_index - 1))

                # Insert raw samples
                sample_rows = []
                for offset, sample_vals in enumerate(samples):
                    curr_idx = first_index + offset
                    ch1 = sample_vals[0] if len(sample_vals) > 0 else None
                    ch2 = sample_vals[1] if len(sample_vals) > 1 else None
                    ch3 = sample_vals[2] if len(sample_vals) > 2 else None
                    sample_rows.append((session_id, self.sensor_id, curr_idx, ch1, ch2, ch3, 0))

                if sample_rows:
                    cursor.executemany("""
                        INSERT OR IGNORE INTO raw_samples (session_id, sensor, sample_index, ch1, ch2, ch3, synced)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, sample_rows)
                    self.last_index = first_index + len(samples) - 1

                # Update session state
                cursor.execute("""
                    INSERT OR REPLACE INTO session_state (sensor, session_id, last_index)
                    VALUES (?, ?, ?)
                """, (self.sensor_id, session_id, self.last_index))

                conn.commit()

                if session_end:
                    print(f"[*] Sensor {self.sensor_id}: Session {session_id} ended.")
                    break

                time.sleep(self.poll_interval)

            except (socket.timeout, ConnectionResetError, BrokenPipeError, OSError) as e:
                if self.sock:
                    try:
                        self.sock.close()
                    except Exception:
                        pass
                    self.sock = None
                time.sleep(1.0)
            except Exception as e:
                print(f"[!] Sensor {self.sensor_id} error: {e}")
                time.sleep(0.5)

        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        conn.close()

    def stop(self):
        self.running = False


def start_ingestion_threads(host='127.0.0.1', port=7100):
    """Start ingestion threads for both Sensor 1 and Sensor 2."""
    init_db()
    t1 = SensorPoller(1, host=host, port=port)
    t2 = SensorPoller(2, host=host, port=port)
    t1.start()
    t2.start()
    return t1, t2


if __name__ == "__main__":
    init_db()
    print("[*] Starting dual sensor ingestion threads (Sensor 1 & Sensor 2)...")
    t1, t2 = start_ingestion_threads()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[*] Stopping ingestion...")
        t1.stop()
        t2.stop()
        t1.join()
        t2.join()