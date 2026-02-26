#!/bin/bash
# NAS Signal Tester - Launcher
# Usage: ./start.sh              (foreground)
#        ./start.sh --background (daemon mode)
#        ./start.sh --once       (single cycle)
#        ./start.sh --ingest FILE.json  (ingest a signal_pack JSON file)

cd "$(dirname "$0")"

# Create virtual environment if needed
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    echo "Setup complete."
else
    source venv/bin/activate
fi

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Ensure directories exist
mkdir -p data reports logs

if [ "$1" = "--once" ]; then
    echo "Running single cycle..."
    python3 runner.py --once
elif [ "$1" = "--ingest" ]; then
    if [ -z "$2" ]; then
        echo "Usage: ./start.sh --ingest <signal_pack.json>"
        exit 1
    fi
    echo "Ingesting signal_pack from $2..."
    python3 runner.py --ingest "$2"
elif [ "$1" = "--background" ]; then
    echo "Starting NAS Signal Tester in background..."
    nohup python3 runner.py > logs/nas_tester_stdout.log 2>&1 &
    PID=$!
    echo $PID > .nas_tester.pid
    echo "Running with PID $PID"
    echo "Logs: tail -f logs/nas_tester.log"
    echo "Stop: ./stop.sh"
else
    echo "Starting NAS Signal Tester..."
    python3 runner.py
fi
