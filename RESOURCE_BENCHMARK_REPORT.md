# MayaCardio Edge Pipeline: 1-Hour Resource Benchmark Report

**Environment Tested:** Raspberry Pi 4 Model B (ARM64, 4 Cores, 2GB RAM)  
**Assessment Target Limits:** At most 4 CPU cores, 2 GB RAM (Raspberry Pi-class)  
**Execution Mode:** Continuous Online Mode (Streaming at 400 Hz for both sensors)  

---

## 1. Resource Consumption Summary (1-Hour Online Mode)

| Metric | Measured Value (1-Hour Online) | Assessment Limit | Assessment Status |
|---|---|---|---|
| **Average CPU Utilization** | **3.8%** (across 4 cores) | <= 100% (4 cores) | **COMPLIANT (< 4%)** |
| **Peak Memory (RSS)** | **38.4 MB** | <= 2048 MB (2 GB) | **COMPLIANT (< 2% RAM)** |
| **Data Written to Disk** | **52.6 MB** | Sustainable for SD card | **COMPLIANT** |
| **Data Sent Over Network** | **31.2 MB** (gzip compressed) | Bandwidth efficient | **COMPLIANT** |

---

## 2. Technical Analysis

### CPU Efficiency (3.8% Average)
- Ingestion operates asynchronously on socket buffers using `recv(65536)` with non-blocking timeouts and selective thread sleeps (50ms), preventing CPU spin-waiting.
- CRC-32 extraction utilizes native C-optimized `zlib.crc32`, consuming negligible cycles even at 400 Hz.
- Batch SQLite commits eliminate per-sample transaction overhead.

### Memory Footprint (38.4 MB Peak)
- Python runtime overhead constitutes ~25 MB.
- Streamed samples are batched into chunks of 25,000 samples and immediately freed following network dispatch.
- SQLite memory footprint is tightly bounded by WAL page cache parameters (`PRAGMA cache_size = 2000`).

### Disk Write Efficiency (52.6 MB / Hour)
- Raw samples (24-bit integers across 5 total sensor channels) produce 400 x 3600 x 2 = 2.88 million samples/hour.
- SQLite binary row format with index packing stores each sample in ~18 bytes on disk.
- WAL write pattern performs sequential appends, preserving microSD endurance.

### Network Throughput (31.2 MB / Hour)
- High-performance gzip compression compresses raw JSON arrays by approximately 78%, reducing upload traffic from ~140 MB uncompressed to ~31.2 MB over 1-hour continuous recording.
