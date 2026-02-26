#!/bin/bash
# Stop NAS Signal Tester daemon
cd "$(dirname "$0")"

if [ -f .nas_tester.pid ]; then
    PID=$(cat .nas_tester.pid)
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping NAS Signal Tester (PID $PID)..."
        kill "$PID"
        rm .nas_tester.pid
        echo "Stopped."
    else
        echo "PID $PID not running. Cleaning up."
        rm .nas_tester.pid
    fi
else
    echo "No PID file found. May not be running."
    echo "Check: ps aux | grep runner.py"
fi
