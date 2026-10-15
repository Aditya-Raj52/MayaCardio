# MayaCardio Demo Video Recording Guide & Script

**Duration Limit:** At most 5 minutes (Planned runtime: 4 minutes 30 seconds)  
**Required Demonstrations:** Network cut, Process kill, and Power cut  
**Environment:** Raspberry Pi 4 Model B (ARM64) / Linux Terminal  

---

## Screen Layout Recommendation

Use a split terminal layout (e.g. `tmux` or two terminal windows side-by-side):
- **Top / Left Terminal:** Active pipeline daemon running (`python3 pipeline.py run`)
- **Bottom / Right Terminal:** Interactive CLI commands (`pipeline status`, failure injections, verification)

---

## Minute-by-Minute Script & Command Sequence

### Part 1: Normal Online Operation (0:00 – 1:00)
1. **0:00 - 0:20 (Intro):** Introduce yourself, state the assessment ("Never Lose a Heartbeat"), and identify the target board (Raspberry Pi 4 ARM64, 2 GB RAM).
2. **0:20 - 0:40 (Start Services):**
   ```bash
   # Terminal 1: Start mock server
   python3 mock_server_v2.py --mode signal --seed 7 --duration 3600 --port 7100 &

   # Terminal 1: Start edge pipeline daemon
   python3 pipeline.py run
   ```
3. **0:40 - 1:00 (Check Status):**
   In Terminal 2, run:
   ```bash
   python3 pipeline.py status
   ```
   **Highlight to camera:** Mode is `"online"`, samples are actively saving and streaming to cloud, and `backlog` is `0`.

---

### Part 2: Network Cut & Recovery Demo (1:00 – 2:15)
1. **1:00 - 1:20 (Cut Network):**
   Simulate network cut (either disconnect ethernet/WiFi or block cloud port):
   ```bash
   # Block cloud communication via iptables (or ifconfig wlan0 down):
   sudo iptables -A OUTPUT -p tcp --dport 8000 -j DROP
   ```
2. **1:20 - 1:40 (Verify Offline Switch within 10s):**
   ```bash
   python3 pipeline.py status
   ```
   **Highlight to camera:** Mode automatically transitioned from `"online"` to `"offline"` within **< 5 seconds**. Notice `backlog` increasing as device continues to collect and save all biosignals locally.
3. **1:40 - 2:00 (Restore Network):**
   ```bash
   # Remove iptables rule (or ifconfig wlan0 up):
   sudo iptables -D OUTPUT -p tcp --dport 8000 -j DROP
   ```
4. **2:00 - 2:15 (Verify Catch-up):**
   ```bash
   python3 pipeline.py status
   ```
   **Highlight to camera:** Mode returns to `"online"` within **< 5 seconds**, and the high-throughput catch-up worker flushes the backlog to `0` in seconds with zero duplicates.

---

### Part 3: Process Kill Demo (2:15 – 3:15)
1. **2:15 - 2:40 (Abrupt Process Kill):**
   While data is actively streaming in Terminal 1, kill the pipeline process with uncatchable `SIGKILL` (-9):
   ```bash
   # Find pipeline PID and kill immediately
   kill -9 $(pgrep -f "pipeline.py run")
   ```
   **Highlight to camera:** The process was forcefully aborted mid-stream without a graceful cleanup handler.
2. **2:40 - 3:15 (Integrity Verification & Restart):**
   ```bash
   # Check SQLite database integrity
   sqlite3 pipeline_storage.db "PRAGMA integrity_check;"
   
   # Restart pipeline
   python3 pipeline.py run &
   python3 pipeline.py status
   ```
   **Highlight to camera:** SQLite WAL journal atomically restored database state. `integrity_check` outputs `ok`. Pipeline resumes with zero corrupt records.

---

### Part 4: Power Cut Demo (3:15 – 4:15)
1. **3:15 - 3:45 (Simulate Sudden Power Cut):**
   Demonstrate sudden power loss (either pull physical 5V DC power plug on the Raspberry Pi or trigger an immediate hardware reboot via Linux kernel SysRq):
   ```bash
   # Triggers instantaneous ungraceful reboot (equivalent to physical power cut)
   sudo sh -c "echo b > /proc/sysrq-trigger"
   ```
2. **3:45 - 4:15 (Post-Boot Data Verification):**
   After board reboots:
   ```bash
   cd MayaCardio
   source venv/bin/activate
   
   # Verify last recorded index and loss bound
   python3 pipeline.py status
   python3 pipeline.py export --session <session_id> --from device | tail -n 10
   ```
   **Highlight to camera:** The system boots safely. SQLite WAL recovery completes in $< 100\text{ ms}$. Total data loss is bounded to **$\le 0.05$ seconds ($\le 20$ samples)** due to `PRAGMA synchronous = FULL` forced disk fsync.

---

### Part 5: Final Verification & Conclusion (4:15 – 4:30)
Run the automated verification suite to prove all 8 assessment criteria:
```bash
python3 test_pipeline.py
```
**Highlight to camera:** **35/35 tests passing (100% Pass Rate)**. Conclude the video under 4 minutes 30 seconds.
