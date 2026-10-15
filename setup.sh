#!/bin/bash
# MayaCardio Edge Pipeline - Fresh Board Setup Script
# Target: Raspberry Pi OS (64-bit) / Debian ARM64 / Ubuntu ARM64

set -e

echo "=========================================================="
echo " MayaCardio Edge Pipeline - Fresh Board Setup"
echo "=========================================================="

# 1. System packages
echo "[*] Updating apt repositories and installing system tools..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv sqlite3 git curl

# 2. Virtual Environment
echo "[*] Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# 3. Python dependencies
echo "[*] Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# 4. Initialize SQLite storage
echo "[*] Initializing local crash-safe SQLite database..."
python3 database.py

# 5. Make CLI executable
chmod +x pipeline.py

echo ""
echo "=========================================================="
echo " Setup complete!"
echo " To run the test suite:"
echo "   source venv/bin/activate"
echo "   python3 test_pipeline.py"
echo ""
echo " To start the mock service:"
echo "   python3 mock_server_v2.py --mode signal --seed 7 --duration 3600 --port 7100 &"
echo ""
echo " To run the edge pipeline:"
echo "   python3 pipeline.py run"
echo "=========================================================="
