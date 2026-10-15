"""
Assessment-compliant Mock Biosignal Server.

Implements the exact protocol from MayaCardio assessment section 6:
- Newline-delimited JSON over TCP, one client at a time
- Request: {"cmd":"read","sensor":1} or {"cmd":"read","sensor":1,"from_index":N}
- Reply: {"session_id","sensor","first_index","count","lost","session_end","crc32","data":[[ch1,ch2,ch3],...]}
- Two sensors: Sensor 1 (3 channels), Sensor 2 (2 channels)
- 400 Hz sampling rate
- CRC32 over data array text from [ to ]
- Supports --mode signal and --mode ramp
- Supports fault injection: corrupt CRC, gaps, duplicate replies, session restart
"""

import argparse
import json
import math
import random
import socket
import sys
import time
import threading
import uuid
import zlib


class SensorBuffer:
    """Circular buffer holding the last 10 seconds of data per sensor (4000 samples at 400Hz)."""
    
    BUFFER_SIZE = 4000  # 10 seconds at 400 Hz
    
    def __init__(self, sensor_id, mode, seed):
        self.sensor_id = sensor_id
        self.mode = mode
        self.rng = random.Random(seed + sensor_id)
        self.current_index = 0
        self.buffer = {}  # index -> [ch1, ch2, ...]
        self.lost_ranges = []
        self.session_id = uuid.uuid4().hex[:8]
        self.session_ended = False
        self.read_cursor = 0  # tracks what the client has read up to
        self.lock = threading.Lock()
    
    def generate_sample(self, index):
        """Generate a sample based on mode."""
        if self.mode == 'ramp':
            if self.sensor_id == 1:
                return [(3 * index + c) % 8388608 for c in range(1, 4)]  # 3 channels
            else:
                return [(2 * index + c) % 8388608 for c in range(1, 3)]  # 2 channels
        else:  # signal mode
            if self.sensor_id == 1:
                # Channel 2: ECG-like with heartbeat peaks
                # HR pattern: 60 bpm min 0-2, 90 bpm min 2-4, 72 bpm min 4-6, repeating
                t = index / 400.0  # time in seconds
                cycle_t = t % 360.0  # 6-minute cycle
                minute = cycle_t / 60.0
                
                if minute < 2:
                    hr = 60
                elif minute < 4:
                    hr = 90
                else:
                    hr = 72
                
                beat_period = 60.0 / hr  # seconds per beat
                phase = (t % beat_period) / beat_period
                
                # ECG-like waveform: sharp peak at phase ~0.0
                if phase < 0.05:
                    ch2 = int(800000 + 400000 * math.exp(-((phase - 0.02) ** 2) / 0.0002))
                elif phase < 0.15:
                    ch2 = int(800000 - 100000 * math.sin((phase - 0.05) * math.pi / 0.1))
                else:
                    ch2 = int(800000 + 50000 * math.sin((phase - 0.15) * math.pi / 0.85))
                
                ch1 = int(ch2 * 0.6)
                ch3 = int(ch2 * 1.3)
                return [ch1, ch2, ch3]
            else:
                # Sensor 2: slowly varying
                t = index / 400.0
                ch1 = int(400000 + 5000 * math.sin(t * 0.1))
                ch2 = int(150000 + 3000 * math.cos(t * 0.07))
                return [ch1, ch2]
    
    def advance(self, count=100):
        """Generate next batch of samples, evicting old ones from buffer."""
        with self.lock:
            evicted_from = None
            evicted_to = None
            
            for i in range(count):
                idx = self.current_index
                self.buffer[idx] = self.generate_sample(idx)
                self.current_index += 1
                
                # Evict samples older than buffer size
                old_idx = idx - self.BUFFER_SIZE
                if old_idx in self.buffer:
                    if old_idx >= self.read_cursor:
                        # Client hasn't read this yet - it's lost
                        if evicted_from is None:
                            evicted_from = old_idx
                        evicted_to = old_idx
                    del self.buffer[old_idx]
            
            if evicted_from is not None:
                self.lost_ranges.append({
                    "from_index": evicted_from,
                    "to_index": evicted_to,
                    "reason": "buffer_overflow"
                })
    
    def read_data(self, from_index=None, max_samples=400):
        """Read data from buffer. Returns (first_index, data, lost, session_end)."""
        with self.lock:
            if from_index is not None:
                start = from_index
            else:
                start = self.read_cursor
            
            data = []
            first_index = start
            lost = list(self.lost_ranges)
            self.lost_ranges.clear()
            
            # Filter lost ranges relevant to this read
            relevant_lost = [l for l in lost if l['to_index'] >= start - self.BUFFER_SIZE]
            
            available_indices = sorted([k for k in self.buffer.keys() if k >= start])
            count = 0
            for idx in available_indices:
                if count >= max_samples:
                    break
                data.append(self.buffer[idx])
                count += 1
            
            if data:
                first_index = available_indices[0]
                self.read_cursor = first_index + len(data)
            
            return first_index, data, relevant_lost, self.session_ended


class MockBiosignalServer:
    """TCP server implementing the assessment mock protocol."""
    
    def __init__(self, host, port, mode, seed, duration):
        self.host = host
        self.port = port
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.sensors = {
            1: SensorBuffer(1, mode, seed),
            2: SensorBuffer(2, mode, seed),
        }
        self.running = True
        self.start_time = None
        
        # Fault injection settings
        self.corrupt_probability = 0.0  # Probability of sending corrupt CRC
        self.duplicate_probability = 0.0  # Probability of sending duplicate reply
        self.gap_probability = 0.0  # Probability of skipping samples
        self.pause_probability = 0.0  # Probability of pausing
    
    def data_generator_thread(self):
        """Continuously generate data at 400 Hz (in batches of 100 every 0.25s)."""
        self.start_time = time.time()
        while self.running:
            elapsed = time.time() - self.start_time
            if self.duration and elapsed > self.duration:
                for s in self.sensors.values():
                    s.session_ended = True
                break
            
            for sensor in self.sensors.values():
                sensor.advance(100)  # 100 samples = 0.25 seconds at 400Hz
            
            time.sleep(0.25)
    
    def compute_crc32(self, data_array):
        """Compute CRC32 over the data array text exactly as it would appear in JSON."""
        data_text = json.dumps(data_array)
        return format(zlib.crc32(data_text.encode('utf-8')) & 0xFFFFFFFF, '08x')
    
    def handle_client(self, client_socket, client_addr):
        """Handle a single client connection."""
        print(f"[+] Client connected from {client_addr}")
        buffer = b""
        last_replies = {}  # sensor -> last reply for duplicate injection
        
        try:
            while self.running:
                data = client_socket.recv(65536)
                if not data:
                    break
                
                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        request = json.loads(line.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    
                    cmd = request.get("cmd")
                    sensor_id = request.get("sensor")
                    from_index = request.get("from_index")
                    
                    if cmd != "read" or sensor_id not in self.sensors:
                        error_reply = json.dumps({"error": "invalid request"}) + "\n"
                        client_socket.sendall(error_reply.encode('utf-8'))
                        continue
                    
                    # Fault: pause
                    if self.pause_probability > 0 and random.random() < self.pause_probability:
                        time.sleep(random.uniform(2, 8))
                    
                    sensor = self.sensors[sensor_id]
                    
                    # Fault: gap injection - skip some samples
                    if self.gap_probability > 0 and random.random() < self.gap_probability:
                        with sensor.lock:
                            skip = random.randint(100, 400)
                            sensor.read_cursor += skip
                    
                    first_index, samples, lost, session_end = sensor.read_data(from_index)
                    
                    if not samples:
                        # No data available yet, send empty
                        reply_obj = {
                            "session_id": sensor.session_id,
                            "sensor": sensor_id,
                            "first_index": first_index,
                            "count": 0,
                            "lost": lost,
                            "session_end": session_end,
                            "crc32": self.compute_crc32([]),
                            "data": []
                        }
                    else:
                        data_array = samples
                        crc = self.compute_crc32(data_array)
                        
                        # Fault: corrupt CRC
                        if self.corrupt_probability > 0 and random.random() < self.corrupt_probability:
                            crc = format(random.randint(0, 0xFFFFFFFF), '08x')
                        
                        reply_obj = {
                            "session_id": sensor.session_id,
                            "sensor": sensor_id,
                            "first_index": first_index,
                            "count": len(data_array),
                            "lost": lost,
                            "session_end": session_end,
                            "crc32": crc,
                            "data": data_array
                        }
                    
                    reply_str = json.dumps(reply_obj) + "\n"
                    
                    # Fault: duplicate reply
                    if self.duplicate_probability > 0 and sensor_id in last_replies and random.random() < self.duplicate_probability:
                        client_socket.sendall(last_replies[sensor_id].encode('utf-8'))
                    
                    client_socket.sendall(reply_str.encode('utf-8'))
                    last_replies[sensor_id] = reply_str
                    
        except (ConnectionResetError, BrokenPipeError, OSError):
            print(f"[-] Client {client_addr} disconnected")
        finally:
            client_socket.close()
            print(f"[*] Client socket closed. Ready for next connection.")
    
    def run(self):
        """Start the mock server."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(1)
            print(f"[*] Biosignal Mock Server (Assessment Protocol) started on {self.host}:{self.port}")
            print(f"[*] Mode: {self.mode}, Seed: {self.seed}, Duration: {self.duration}s")
            print(f"[*] Session IDs: Sensor1={self.sensors[1].session_id}, Sensor2={self.sensors[2].session_id}")
            
            # Start data generation thread
            gen_thread = threading.Thread(target=self.data_generator_thread, daemon=True)
            gen_thread.start()
            
            # Wait a moment for initial data
            time.sleep(0.5)
            
            while self.running:
                server_socket.settimeout(1.0)
                try:
                    client_socket, client_addr = server_socket.accept()
                    self.handle_client(client_socket, client_addr)
                except socket.timeout:
                    continue
                    
        except KeyboardInterrupt:
            print("\n[*] Shutting down mock server.")
        finally:
            self.running = False
            server_socket.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Assessment-Compliant Biosignal Mock TCP Server")
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host address')
    parser.add_argument('--port', type=int, default=7100, help='TCP port')
    parser.add_argument('--mode', type=str, default='ramp', choices=['signal', 'ramp'], help='Data mode')
    parser.add_argument('--seed', type=int, default=7, help='Random seed')
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds (0 for infinite)')
    
    # Fault injection flags
    parser.add_argument('--corrupt-rate', type=float, default=0.0, help='CRC corruption probability (0-1)')
    parser.add_argument('--duplicate-rate', type=float, default=0.0, help='Duplicate reply probability (0-1)')
    parser.add_argument('--gap-rate', type=float, default=0.0, help='Gap injection probability (0-1)')
    parser.add_argument('--pause-rate', type=float, default=0.0, help='Pause probability (0-1)')
    
    args = parser.parse_args()
    
    server = MockBiosignalServer(args.host, args.port, args.mode, args.seed, args.duration)
    server.corrupt_probability = args.corrupt_rate
    server.duplicate_probability = args.duplicate_rate
    server.gap_probability = args.gap_rate
    server.pause_probability = args.pause_rate
    
    server.run()
