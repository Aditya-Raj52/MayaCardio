# MayaCardio Edge Pipeline: Failure Tests Evidence & Results

**Execution Date:** 2026-10-15 21:02:41 UTC  
**Environment:** ARM64 / Raspberry Pi Class Linux Environment  

| Category | Test Scenario | Status | Result & Empirical Evidence |
|---|---|---|---|
| Sensor Fault | CRC-32 Corruption Injection | **PASS** | Expected 'deadbeef', computed '4502ed5e'. Corrupt packet successfully caught & discarded. |
| Data Safety | Duplicate Packet Rejection | **PASS** | Duplicate sample index 0 rejected by composite PK. Data remained unchanged (ch1=10). |
| Crash Safety | Process Kill & WAL Recovery | **PASS** | PRAGMA integrity_check: ok. Exactly 500/500 records recovered atomically. |
| Power Safety | Power Cut Data Loss Window | **PASS** | Avg commit latency: 0.50ms, Max: 1.24ms. Maximum lost data window = 1.2ms (<50ms guarantee, <= 1 samples). |
| Network Switch | Network Loss Detection (<10s) | **PASS** | Switched from ONLINE to OFFLINE in 1.52s (Assessment requirement: <=10s). |
| Catch-Up Sync | 2-Hour Outage Catch-Up Speed | **PASS** | With 25000 gzipped samples/chunk, 5.76M samples flush in 1.0 min (Assessment requirement: <= 15 minutes). Zero duplicates via primary key. |
| Audit Integrity | Loss Log Recording | **PASS** | Unreceived samples logged with exact range [4000, 4399] and reason 'buffer_overflow'. No silent loss. |
