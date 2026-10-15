# MayaCardio Edge Biosignal Pipeline

A robust, crash-resilient edge pipeline designed for continuous bedside biosignal recording under adverse operating conditions (unannounced power cuts, sudden network drops, process termination, and sensor faults). Built strictly for Raspberry Pi 4 ARM64 environments and free-tier cloud deployment in compliance with the **MayaCardio Software Systems Engineering Candidate Assessment**.

---

## 1. System Overview

```
                      +---------------------------------------+
                      |       biosignal-mock (port 7100)      |
                      |  Sensor 1 (3ch) & Sensor 2 (2ch) @400Hz|
                      +-------------------+-------------------+
                                          | Newline-delimited JSON
                                          v
+---------------------------------------------------------------------------------+
|                                 EDGE DEVICE (ARM64)                             |
|                                                                                 |
|  +--------------------+    TCP     +-----------------------------------------+  |
|  | Poller Sensor 1    | ---------> | Ingestion Engine (ingestion.py)         |  |
|  +--------------------+            | - Bracket-depth CRC-32 extraction       |  |
|  +--------------------+    TCP     | - Re-read on error (from_index)         |  |
|  | Poller Sensor 2    | ---------> | - Session restart & loss auditing       |  |
|  +--------------------+            +--------------------+--------------------+  |
|                                                         | Atomic commits        |
|                                                         v                       |
|                                    +-----------------------------------------+  |
|                                    | SQLite WAL Storage (database.py)        |  |
|                                    | PRAGMA synchronous = FULL               |  |
|                                    | PRAGMA journal_mode = WAL               |  |
|                                    +--------------------+--------------------+  |
|                                                         |                       |
|                          +------------------------------+--------+              |
|                          | Offline Mode                          | Online Mode  |
|                          v                                       v              |
|  +-----------------------------------------+   +-----------------------------+  |
|  | Edge Analytics Worker (analytics.py)    |   | Catch-Up Syncer (sync.py)   |  |
|  | - 1-second windowing (400 samples)      |   | - 25,000 samples/batch gzip |  |
|  | - Dual neighbor peak detection          |   | - Fast HTTP POST upload     |  |
|  | - 5-sec rolling HR & Sensor 2 magnitude |   | - Auto fallback <10s        |  |
|  +-----------------------------------------+   +--------------+--------------+  |
+---------------------------------------------------------------|-----------------+
                                                                | HTTPS / Gzip
                                                                v
+---------------------------------------------------------------------------------+
|                                CLOUD SERVICE (Free Tier)                        |
|                                                                                 |
|  +---------------------+   +---------------------+   +-----------------------+  |
|  | /health (Heartbeat) |   | /ingest (Streaming) |   | /results & /export    |  |
|  +---------------------+   +---------------------+   +-----------------------+  |
|  Online Processing within 5s across cloud windows                               |
+---------------------------------------------------------------------------------+
```

---

## 2. Fresh Board Installation (Raspberry Pi / ARM64)

The pipeline is tested and designed to run on a fresh Raspberry Pi 4 Model B (4 cores, 2 GB RAM) running Raspberry Pi OS 64-bit or Ubuntu ARM64.

### Step 1: Clone the Repository
```bash
git clone https://github.com/your-username/MayaCardio.git
cd MayaCardio
```

### Step 2: One-Step Automated Setup
```bash
chmod +x setup.sh
./setup.sh
```

Or manually:
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv sqlite3

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 database.py
```

---

## 3. Running the Pipeline

### Step 1: Start Mock Biosignal Server
```bash
# Ramp mode (for mathematical verification):
python3 mock_server_v2.py --mode ramp --seed 7 --duration 3600 --port 7100 &

# Signal mode (realistic ECG waveform):
python3 mock_server_v2.py --mode signal --seed 7 --duration 3600 --port 7100 &
```

### Step 2: Start Edge Daemon
```bash
python3 pipeline.py run
```
This spawns:
1. Dual ingestion threads polling Sensor 1 (3 channels) and Sensor 2 (2 channels) at 400 Hz.
2. Network synchronizer checking connectivity every 2.5 seconds and flushing backlog in 25,000-sample gzipped batches.

---

## 4. Assessment CLI Commands

The assessment requires one unified tool, `pipeline`:

### 1. `pipeline status`
Prints current mode, samples saved, samples in the cloud, and backlog as a single JSON line:
```bash
python3 pipeline.py status
```
**Output Example:**
```json
{"mode": "offline", "samples_saved": 84200, "samples_in_cloud": 42000, "backlog": 42200}
```

### 2. `pipeline export --session ID --from device` (or `--from cloud`)
Outputs saved samples as JSON lines, followed by loss audit logs:
```bash
python3 pipeline.py export --session 5f0c2a9e --from device
```
**Output Format:**
```json
{"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
{"sensor": 1, "index": 120001, "values": [708, 1180, 1534]}
{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
```

### 3. `pipeline results --session ID`
Outputs Section 3 processing metrics per window:
```bash
python3 pipeline.py results --session 5f0c2a9e
```
**Output Format:**
```json
{"session_id": "5f0c2a9e", "window_id": 0, "mode": "offline", "missing_1": 0, "missing_2": 0, "complete": true, "s1_ch1": {"mean": 480.0, "min": 400.0, "max": 560.0}, "s1_ch2": {"mean": 800.0, "min": 650.0, "max": 1200.0}, "s1_ch3": {"mean": 1040.0, "min": 850.0, "max": 1560.0}, "s2_ch1": {"mean": 400000.0, "min": 395000.0, "max": 405000.0}, "s2_ch2": {"mean": 150000.0, "min": 147000.0, "max": 153000.0}, "magnitude": 427200.5, "heart_rate": 72.0}
```

### 4. `pipeline process --session ID`
Computes metrics across all windows for a given session offline:
```bash
python3 pipeline.py process --session 5f0c2a9e --mode offline
```

---

## 5. Free-Tier Cloud Deployment

The repository includes a ready-to-deploy cloud backend in `cloud_server.py`.

### Deploying to Render / Railway / Fly.io (Free Tier)
1. Fork or push this repository to GitHub.
2. Link the repository to [Render.com](https://render.com) (Web Service, Free Instance Type) or [Railway.app](https://railway.app).
3. Set the start command:
   ```bash
   uvicorn cloud_server:app --host 0.0.0.0 --port $PORT
   ```
4. On your Raspberry Pi, set the cloud endpoint environment variable:
   ```bash
   export CLOUD_ENDPOINT="https://your-service.onrender.com/ingest"
   ```

---

## 6. Verification & Self-Testing

To run the complete 35-test verification suite:
```bash
python3 test_pipeline.py
```
**Validation Coverage:**
- Database schema, WAL mode, synchronous=FULL, and crash persistence
- Accurate CRC-32 bracket-depth extraction
- Section 3 analytics (both neighbors peak detection, magnitude, rolling HR)
- CLI contract verification (`status`, `export`, `results`)
- End-to-end live ingestion from `mock_server_v2.py`
- Catch-up throughput (>6,400 samples/s) and fast switch latency (<10s)

---

## 7. AI Disclosure

In accordance with Section 5 rules:
*AI tools (Google Antigravity / Gemini 3.8 Flash) were utilized during development for code restructuring, test harness generation, and technical documentation drafting.*
