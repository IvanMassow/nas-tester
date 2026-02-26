"""
NAS Signal Tester - Scanner
Polls RSS feeds for NAS signal_packs and ingests them.
Also supports direct JSON file ingestion for development/testing.
"""
import json
import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests

from db import get_conn
from config import RSS_FEEDS, REPORT_TITLE_PREFIX
from position_manager import open_positions_from_signal

logger = logging.getLogger("nas.scanner")


def fetch_rss(url):
    """Fetch and parse an RSS feed. Returns list of items."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        items = []
        for item in root.findall(".//item"):
            items.append({
                "title": (item.findtext("title") or "").strip(),
                "link": (item.findtext("link") or "").strip(),
                "description": (item.findtext("description") or "").strip(),
                "guid": (item.findtext("guid") or "").strip(),
                "pubDate": (item.findtext("pubDate") or "").strip(),
            })
        return items
    except Exception as e:
        logger.error("RSS fetch failed for {}: {}".format(url, e))
        return []


def extract_signal_pack(description):
    """Extract signal_pack JSON from RSS item description.
    The signal_pack may be embedded as:
    1. A JSON code block in markdown (```json ... ```)
    2. A raw JSON object in the description
    3. A JSON object within HTML tags
    """
    if not description:
        return None

    # Try 1: Look for ```json ... ``` blocks
    import re
    json_block = re.search(r'```json\s*(\{.*?\})\s*```', description, re.DOTALL)
    if json_block:
        try:
            return json.loads(json_block.group(1))
        except json.JSONDecodeError:
            pass

    # Try 2: Look for a signal_pack JSON object directly
    # Find the first balanced { ... } that contains "signal_pack" or "primary_asset_signal"
    for marker in ['"signal_pack"', '"primary_asset_signal"', '"direction_bias"', '"cascade_map"']:
        idx = description.find(marker)
        if idx >= 0:
            # Walk backwards to find the opening {
            start = description.rfind('{', 0, idx)
            if start >= 0:
                # Try to parse from this position
                depth = 0
                for i in range(start, len(description)):
                    if description[i] == '{':
                        depth += 1
                    elif description[i] == '}':
                        depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(description[start:i + 1])
                        except json.JSONDecodeError:
                            break
                        break

    # Try 3: The entire description might be JSON
    try:
        parsed = json.loads(description)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # Try 4: Strip HTML and try again
    clean = re.sub(r'<[^>]+>', '', description).strip()
    try:
        parsed = json.loads(clean)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    return None


def normalise_signal_pack(raw):
    """Normalise a signal_pack JSON object into our standard structure.
    Handles both the full signal_pack wrapper and direct signal objects.
    """
    if not raw or not isinstance(raw, dict):
        return None

    # If the raw object IS a signal_pack (has metadata, primary_asset_signal, etc.)
    if "primary_asset_signal" in raw and "cascade_map" in raw:
        return raw

    # If wrapped in a signal_pack key
    if "signal_pack" in raw:
        return raw["signal_pack"]

    # If it's an 8A output with signal_pack inside
    for key in ["workflow_8A_output", "workflow8a_output"]:
        if key in raw and isinstance(raw[key], dict):
            inner = raw[key]
            if "signal_pack" in inner:
                return inner["signal_pack"]

    # If it contains enough NAS fields to be usable directly
    if "direction_bias" in raw or "pressure_index" in raw:
        return raw

    return None


def derive_signal_id(rss_guid=None, signal_pack=None, title=None):
    """Generate a unique signal ID from available data."""
    if rss_guid:
        return "NAS-" + hashlib.md5(rss_guid.encode()).hexdigest()[:8].upper()

    # Hash from content
    content = json.dumps(signal_pack, sort_keys=True) if signal_pack else (title or "unknown")
    return "NAS-" + hashlib.md5(content.encode()).hexdigest()[:8].upper()


def ingest_signal_pack(signal_pack, rss_guid=None, title=None, report_url=None,
                        published_date=None):
    """Ingest a normalised signal_pack into the database and open positions.
    Returns the signal_id if new, or None if already exists.
    """
    signal_id = derive_signal_id(rss_guid, signal_pack, title)

    conn = get_conn()
    # Check if already ingested
    existing = conn.execute(
        "SELECT 1 FROM signals WHERE signal_id = ?", (signal_id,)
    ).fetchone()
    if existing:
        conn.close()
        logger.debug("Signal {} already ingested, skipping".format(signal_id))
        return None

    # Extract primary asset signal fields
    pas = signal_pack.get("primary_asset_signal", {}) or {}
    metadata = signal_pack.get("metadata", {}) or {}

    primary_asset = (metadata.get("primary_asset_name") or
                     metadata.get("ticker") or
                     pas.get("primary_asset_name") or
                     signal_pack.get("primary_asset_name") or
                     signal_pack.get("primary_asset") or
                     "Unknown")

    direction_bias = pas.get("direction_bias", signal_pack.get("direction_bias"))
    pressure_index = pas.get("pressure_index", signal_pack.get("pressure_index"))
    acceleration_delta = pas.get("acceleration_delta", signal_pack.get("acceleration_delta"))
    signal_strength = pas.get("signal_strength", signal_pack.get("signal_strength"))
    decay_window = pas.get("decay_window_estimate_hours",
                           signal_pack.get("decay_window_hours",
                           metadata.get("decay_window_hours")))

    driver_clusters = pas.get("top_driver_clusters", [])
    cascade_map = signal_pack.get("cascade_map", [])
    candidate_trades = signal_pack.get("candidate_trades", [])

    now = datetime.now(timezone.utc).isoformat()

    conn.execute("""
        INSERT INTO signals
        (signal_id, rss_guid, title, report_url, published_date, ingested_at,
         primary_asset, direction_bias, pressure_index, acceleration_delta,
         signal_strength, decay_window_hours, top_driver_clusters,
         signal_pack_json, cascade_count, position_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """, (
        signal_id, rss_guid, title, report_url, published_date, now,
        primary_asset, direction_bias, pressure_index, acceleration_delta,
        signal_strength, decay_window, json.dumps(driver_clusters),
        json.dumps(signal_pack),
        len(cascade_map)
    ))
    conn.commit()
    conn.close()

    logger.info("Ingested signal {}: {} {} (pressure={}, strength={})".format(
        signal_id, primary_asset, direction_bias, pressure_index, signal_strength))

    # Open positions from cascade map
    positions_opened = open_positions_from_signal(
        signal_id=signal_id,
        cascade_map=cascade_map,
        candidate_trades=candidate_trades,
        decay_window_hours=decay_window,
        signal_strength=signal_strength,
    )

    # Update position count
    conn = get_conn()
    conn.execute(
        "UPDATE signals SET position_count = ? WHERE signal_id = ?",
        (positions_opened, signal_id)
    )
    conn.commit()
    conn.close()

    return signal_id


def scan():
    """Poll all RSS feeds for new signal_packs. Returns count of new signals."""
    if not RSS_FEEDS:
        logger.debug("No RSS feeds configured")
        return 0

    new_count = 0
    for feed_url in RSS_FEEDS:
        items = fetch_rss(feed_url)
        logger.info("RSS: {} items from {}".format(len(items), feed_url[:60]))

        for item in items:
            title = item.get("title", "")
            # Only process items that look like NAS signal packs
            if REPORT_TITLE_PREFIX and REPORT_TITLE_PREFIX.lower() not in title.lower():
                continue

            rss_guid = item.get("guid") or item.get("link") or ""

            # Check if already processed
            conn = get_conn()
            if rss_guid:
                existing = conn.execute(
                    "SELECT 1 FROM signals WHERE rss_guid = ?", (rss_guid,)
                ).fetchone()
                if existing:
                    conn.close()
                    continue
            conn.close()

            # Extract signal_pack from description
            raw = extract_signal_pack(item.get("description", ""))
            if not raw:
                logger.debug("No signal_pack found in item: {}".format(title[:60]))
                continue

            signal_pack = normalise_signal_pack(raw)
            if not signal_pack:
                logger.warning("Could not normalise signal_pack from: {}".format(title[:60]))
                continue

            sid = ingest_signal_pack(
                signal_pack=signal_pack,
                rss_guid=rss_guid,
                title=title,
                report_url=item.get("link"),
                published_date=item.get("pubDate"),
            )
            if sid:
                new_count += 1

    return new_count


def ingest_from_file(filepath):
    """Ingest a signal_pack from a JSON file. For development/testing."""
    try:
        with open(filepath, "r") as f:
            raw = json.load(f)
    except Exception as e:
        logger.error("Failed to read JSON file {}: {}".format(filepath, e))
        return None

    signal_pack = normalise_signal_pack(raw)
    if not signal_pack:
        # Maybe the file IS the signal_pack already
        if isinstance(raw, dict) and ("cascade_map" in raw or "direction_bias" in raw):
            signal_pack = raw
        else:
            logger.error("Could not extract signal_pack from {}".format(filepath))
            return None

    import os
    title = "Manual ingest: " + os.path.basename(filepath)
    sid = ingest_signal_pack(
        signal_pack=signal_pack,
        title=title,
    )
    return sid


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if len(sys.argv) > 1:
        from db import init_db
        init_db()
        sid = ingest_from_file(sys.argv[1])
        if sid:
            print("Ingested signal: {}".format(sid))
        else:
            print("Ingestion failed or already exists")
    else:
        from db import init_db
        init_db()
        new = scan()
        print("Scan complete: {} new signals".format(new))
