"""
NAS Signal Tester - Runner
Main daemon that orchestrates scanning, tracking, scoring, and reporting.
"""
import os
import sys
import time
import logging
import signal
import shutil
import subprocess
from datetime import datetime, timezone

from db import init_db
from scanner import scan, ingest_from_file
from tracker import track_prices
from scorer import score_all
from report_html import write_report, write_summary_json
from analytics import generate_claude_briefing
from config import (
    SCAN_INTERVAL, TRACK_INTERVAL, SCORE_INTERVAL,
    REPORT_INTERVAL, REPORTS_DIR, LOGS_DIR, BASE_DIR
)

# Configure logging
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "nas_tester.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("nas.runner")

# State
running = True


def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received. Stopping gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def push_to_github():
    """Push latest report and summary to GitHub Pages."""
    try:
        # Check if we're in a git repo
        result = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            cwd=BASE_DIR, capture_output=True, timeout=10
        )
        if result.returncode != 0:
            logger.debug("Not in a git repo, skipping push")
            return False

        subprocess.run(
            ["git", "add", "index.html", "reports/latest.html", "summary.json"],
            cwd=BASE_DIR, capture_output=True, check=True
        )
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        subprocess.run(
            ["git", "commit", "-m", "Update report " + now_str],
            cwd=BASE_DIR, capture_output=True, check=True
        )
        result = subprocess.run(
            ["git", "push"],
            cwd=BASE_DIR, capture_output=True, timeout=30
        )
        if result.returncode == 0:
            logger.info("Report pushed to GitHub Pages")
            return True
        else:
            logger.warning("Git push failed: " + result.stderr.decode()[:200])
            return False
    except subprocess.CalledProcessError as e:
        if "nothing to commit" in (e.stderr or b"").decode():
            logger.debug("No changes to push")
            return True
        logger.warning("Git push error: " + str(e))
        return False
    except Exception as e:
        logger.warning("Push to GitHub failed: " + str(e))
        return False


def generate_and_push():
    """Generate report + summary JSON and push."""
    try:
        path = write_report()
        write_summary_json()
        push_to_github()
        return path
    except Exception as e:
        logger.error("Report generation failed: {}".format(e), exc_info=True)
        return None


def run():
    """Main daemon loop."""
    logger.info("=" * 60)
    logger.info("NAS Signal Tester starting up")
    logger.info("=" * 60)

    init_db()
    logger.info("Database initialised")

    last_scan = 0
    last_track = 0
    last_score = 0
    last_report = 0

    # Initial scan
    logger.info("Running initial scan...")
    try:
        new = scan()
        last_scan = time.time()
        logger.info("Initial scan: {} new signals".format(new))
    except Exception as e:
        logger.error("Initial scan failed: {}".format(e), exc_info=True)

    # Initial price track
    logger.info("Running initial price track...")
    try:
        tracked = track_prices()
        last_track = time.time()
        logger.info("Initial track: {} positions".format(tracked))
    except Exception as e:
        logger.error("Initial track failed: {}".format(e), exc_info=True)

    # Initial scoring
    logger.info("Running initial scoring...")
    try:
        scored = score_all()
        last_score = time.time()
        logger.info("Initial score: {} positions".format(scored))
    except Exception as e:
        logger.error("Initial score failed: {}".format(e), exc_info=True)

    # Generate initial report
    logger.info("Generating initial report...")
    try:
        path = generate_and_push()
        last_report = time.time()
        logger.info("Initial report: {}".format(path))
    except Exception as e:
        logger.error("Initial report failed: {}".format(e), exc_info=True)

    logger.info("Entering main loop. Ctrl+C to stop.")
    logger.info("  Scan: {}min | Track: {}min | Score: {}min | Report: {}h".format(
        SCAN_INTERVAL // 60, TRACK_INTERVAL // 60,
        SCORE_INTERVAL // 60, REPORT_INTERVAL // 3600
    ))

    while running:
        now = time.time()

        # Scan for new signals
        if now - last_scan >= SCAN_INTERVAL:
            try:
                new = scan()
                last_scan = now
                if new > 0:
                    logger.info("Found {} new signal(s)".format(new))
                    # Track prices and score immediately after new signals
                    track_prices()
                    last_track = now
                    score_all()
                    last_score = now
                    generate_and_push()
                    last_report = now
            except Exception as e:
                logger.error("Scan error: {}".format(e), exc_info=True)

        # Track prices
        if now - last_track >= TRACK_INTERVAL:
            try:
                tracked = track_prices()
                last_track = now
            except Exception as e:
                logger.error("Track error: {}".format(e), exc_info=True)

        # Score positions
        if now - last_score >= SCORE_INTERVAL:
            try:
                scored = score_all()
                last_score = now
                if scored > 0:
                    generate_and_push()
                    last_report = now
            except Exception as e:
                logger.error("Score error: {}".format(e), exc_info=True)

        # Heartbeat report
        if now - last_report >= REPORT_INTERVAL:
            try:
                generate_and_push()
                last_report = now
            except Exception as e:
                logger.error("Report error: {}".format(e), exc_info=True)

        # Sleep in small increments for signal responsiveness
        for _ in range(60):
            if not running:
                break
            time.sleep(1)

    logger.info("NAS Signal Tester stopped.")


def run_once():
    """Run a single cycle (testing / manual invocation)."""
    init_db()
    logger.info("Running single cycle...")

    new = scan()
    logger.info("Scan: {} new signals".format(new))

    tracked = track_prices()
    logger.info("Track: {} positions".format(tracked))

    scored = score_all()
    logger.info("Score: {} positions".format(scored))

    path = generate_and_push()
    logger.info("Report: {}".format(path))

    briefing = generate_claude_briefing()
    print("\n" + briefing)

    return path


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        run_once()
    elif len(sys.argv) > 1 and sys.argv[1] == "--ingest":
        # Manual JSON ingestion mode
        if len(sys.argv) < 3:
            print("Usage: python3 runner.py --ingest <file.json>")
            sys.exit(1)
        init_db()
        sid = ingest_from_file(sys.argv[2])
        if sid:
            print("Ingested signal: {}".format(sid))
            tracked = track_prices()
            print("Tracked {} positions".format(tracked))
            scored = score_all()
            print("Scored {} positions".format(scored))
            path = write_report()
            print("Report: {}".format(path))
        else:
            print("Ingestion failed or already exists")
    else:
        run()
