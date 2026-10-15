import argparse
import json
import socket
import sys
import time

def load_dataset(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading dataset file '{filepath}': {e}", file=sys.stderr)
        sys.exit(1)

def run_server(host, port, mode, seed, duration, json_path):
    dataset = load_dataset(json_path)
    
    recording_key = mode.upper() if mode.upper() in dataset else 'S1'
    packets = dataset.get(recording_key, [])
    
    if not packets:
        print(f"No packets found for mode/recording '{recording_key}'", file=sys.stderr)
        sys.exit(1)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((host, port))
        server_socket.listen(1)
        print(f"[*] Biosignal Mock Server started on {host}:{port}")
        print(f"[*] Serving recording '{recording_key}' (Seed: {seed}, Duration: {duration}s)")
        print("[*] Waiting for client connection...")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"[+] Connection accepted from {client_address}")
            
            start_time = time.time()
            try:
                # Stream packets continuously or up to the specified duration
                while True:
                    if duration and (time.time() - start_time) > duration:
                        print("[*] Duration limit reached. Closing client stream.")
                        break
                        
                    for packet in packets:
                        # Serializing given packet to JSON line and send over TCP
                        payload = json.dumps({"recording": recording_key, "packet": packet}) + "\n"
                        client_socket.sendall(payload.encode('utf-8'))
                        
                        # real-time streaming pacing (adjust sleep as needed)
                        time.sleep(0.05)
                        
            except (ConnectionResetError, BrokenPipeError):
                print("[-] Client disconnected abruptly.")
            finally:
                client_socket.close()
                print("[*] Client socket closed. Ready for next connection.\n")
                
    except KeyboardInterrupt:
        print("\n[*] Shutting down mock server.")
    finally:
        server_socket.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Biosignal Mock TCP Server")
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host address to bind to')
    parser.add_argument('--port', type=int, default=8080, help='TCP port to listen on')
    parser.add_argument('--mode', type=str, default='S1', help='Signal mode or recording name (e.g., S1, S2)')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    parser.add_argument('--duration', type=int, default=0, help='Stream duration in seconds (0 for infinite)')
    parser.add_argument('--file', type=str, default='MayaCardio_Signal_Processing_Case_Data_ppg.json', help='Path to JSON dataset')

    args = parser.parse_args()
    run_server(args.host, args.port, args.mode, args.seed, args.duration, args.file)