"""
NAS Signal Tester - Scanner
Polls RSS feeds for NAS signal reports and ingests them.
Supports three ingestion methods:
1. JSON signal_pack embedded in RSS description
2. HTML report parsing (fetches report URL, extracts structured data)
3. Direct JSON file ingestion for development/testing
"""
import json
import hashlib
import logging
import re
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


def parse_html_report(url):
    """Fetch a NAS report page and extract signal data from structured HTML.
    Returns a signal_pack dict or None.

    Parses:
    - Section 2: Primary Asset Signal Dashboard (table with Pressure Index, etc.)
    - Section 5: Cascade Map (bullet list)
    - Section 6: Candidate Trades (bullet list)
    """
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.error("Failed to fetch report page {}: {}".format(url, e))
        return None

    # --- Parse Primary Asset Signal Dashboard (Section 2) ---
    # Look for table rows with Metric/Value pattern
    pas = {}

    # Extract from HTML tables: find <td>Metric</td><td>Value</td> patterns
    table_rows = re.findall(
        r'<td[^>]*>\s*(.*?)\s*</td>\s*<td[^>]*>\s*(.*?)\s*</td>',
        html, re.DOTALL | re.IGNORECASE
    )
    field_map = {
        "pressure index": "pressure_index",
        "direction bias": "direction_bias",
        "acceleration delta": "acceleration_delta",
        "signal strength": "signal_strength",
        "decay window": "decay_window_estimate_hours",
        "estimated decay window": "decay_window_estimate_hours",
        "decay window estimate": "decay_window_estimate_hours",
        "entity name": "primary_asset_name",
        "instrument type": "instrument_type",
    }

    for metric_raw, value_raw in table_rows:
        metric = re.sub(r'<[^>]+>', '', metric_raw).strip().lower()
        value = re.sub(r'<[^>]+>', '', value_raw).strip()

        for key, field in field_map.items():
            if key in metric:
                if field in ("pressure_index", "acceleration_delta"):
                    try:
                        # Handle "+28" or "-12.5" or "92"
                        pas[field] = float(value.replace('+', ''))
                    except ValueError:
                        pas[field] = value
                elif field == "decay_window_estimate_hours":
                    # Extract number from "24 hours" or "24-hour" or just "24"
                    m = re.search(r'(\d+)', value)
                    if m:
                        pas[field] = float(m.group(1))
                else:
                    pas[field] = value
                break

    if not pas.get("direction_bias") and not pas.get("pressure_index"):
        # Try extracting from body text: "Pressure Index 92, Direction Bias UP"
        pi_match = re.search(r'Pressure\s+Index\s+(\d+(?:\.\d+)?)', html, re.IGNORECASE)
        if pi_match:
            pas["pressure_index"] = float(pi_match.group(1))

        dir_match = re.search(r'Direction\s+Bias\s+(UP|DOWN|Bullish|Bearish)', html, re.IGNORECASE)
        if dir_match:
            pas["direction_bias"] = dir_match.group(1)

        accel_match = re.search(r'Acceleration\s+Delta\s+([+-]?\d+(?:\.\d+)?)', html, re.IGNORECASE)
        if accel_match:
            pas["acceleration_delta"] = float(accel_match.group(1))

        str_match = re.search(r'signal\s+strength[^.]*?(High|Moderate|Low)', html, re.IGNORECASE)
        if str_match:
            pas["signal_strength"] = str_match.group(1).capitalize()

        decay_match = re.search(r'decay\s+window[^.]*?(\d+)[\s-]*hour', html, re.IGNORECASE)
        if decay_match:
            pas["decay_window_estimate_hours"] = float(decay_match.group(1))

    if not pas.get("pressure_index"):
        logger.warning("Could not extract primary asset signal from report")
        return None

    # --- Extract primary asset name from title or entity name ---
    title_match = re.search(
        r'Narrative\s+Asset\s+Signal\s+Brief:\s*(.+?)\s*\|',
        html, re.IGNORECASE
    )
    primary_asset_name = (
        pas.get("primary_asset_name") or
        (title_match.group(1).strip() if title_match else None) or
        "Unknown"
    )

    # --- Extract report ID from title [XXXX] ---
    report_id_match = re.search(r'\[([A-Z0-9]{4})\]', html)
    report_id = report_id_match.group(1) if report_id_match else None

    # --- Parse Cascade Map (Section 5) ---
    # Pattern: "Category: expected DIRECTION. Instruments: TICKER1, TICKER2."
    cascade_map = []
    cascade_section = re.search(
        r'(?:Section\s+5|Cascade\s+Map)(.*?)(?:Section\s+6|Candidate\s+Trades|Provenance)',
        html, re.DOTALL | re.IGNORECASE
    )
    if cascade_section:
        cascade_text = cascade_section.group(1)
        # Match: <li><strong>Category:</strong> expected UP. Instruments: XLE, OIH.</li>
        entries = re.findall(
            r'<strong>\s*([^<]+?)\s*:?\s*</strong>\s*:?\s*expected\s+(UP|DOWN)\.'
            r'\s*Instruments?:\s*([^.<]+)',
            cascade_text, re.IGNORECASE
        )
        for category_raw, direction, instruments_raw in entries:
            category = re.sub(r'<[^>]+>', '', category_raw).strip().rstrip(':')
            instruments = []
            if instruments_raw:
                instruments = [t.strip() for t in
                               re.sub(r'<[^>]+>', '', instruments_raw).strip().split(',')
                               if t.strip()]

            cascade_entry = {
                "exposure_category": category.lower(),
                "expected_direction": direction.upper(),
                "magnitude": "Moderate",
                "lag_hours": 0,
            }
            if instruments:
                cascade_entry["instruments"] = instruments
            cascade_map.append(cascade_entry)

    if not cascade_map:
        # Fallback: simpler pattern without HTML tags
        cascade_matches = re.findall(
            r'([A-Za-z][A-Za-z &/]+?):\s*expected\s+(UP|DOWN)\b[^.]*?'
            r'(?:Instruments?:\s*([^.]+))?',
            html, re.IGNORECASE
        )
        for category, direction, instruments_raw in cascade_matches:
            category = category.strip().rstrip(':')
            instruments = []
            if instruments_raw:
                instruments = [t.strip() for t in instruments_raw.strip().split(',')
                               if t.strip() and len(t.strip()) <= 6]
            cascade_entry = {
                "exposure_category": category.lower(),
                "expected_direction": direction.upper(),
                "magnitude": "Moderate",
                "lag_hours": 0,
            }
            if instruments:
                cascade_entry["instruments"] = instruments
            cascade_map.append(cascade_entry)

    # --- Parse Candidate Trades (Section 6) ---
    candidate_trades = []
    trades_section = re.search(
        r'(?:Section\s+6|Candidate\s+Trades)(.*?)(?:Section\s+7|Provenance|Evidence)',
        html, re.DOTALL | re.IGNORECASE
    )
    if trades_section:
        trades_text = trades_section.group(1)
        # Look for instrument entries with triggers
        trade_matches = re.findall(
            r'(?:<strong>|<b>)?\s*([A-Z][A-Z0-9 ]{1,15})\s*(?:</strong>|</b>)?:?\s*'
            r'(?:If[^.]+?),?\s*'
            r'(?:[^.]*?expected to move (up|down)[^.]*?)?'
            r'(?:Trigger:\s*([^.]+)\.)?'
            r'[^.]*?(?:Invalidation:\s*([^.]+)\.)?',
            trades_text, re.IGNORECASE | re.DOTALL
        )
        for instr, direction, trigger, invalidation in trade_matches:
            instr = instr.strip()
            if len(instr) <= 6:  # Likely a ticker
                candidate_trades.append({
                    "instrument": instr,
                    "direction": direction.upper() if direction else None,
                    "trigger_condition": trigger.strip() if trigger else None,
                    "invalidation_condition": invalidation.strip() if invalidation else None,
                })

    # --- Extract driver clusters ---
    driver_clusters = []
    # Look for driver cluster names in bullet lists near "driver" sections
    driver_section = re.search(
        r'(?:Section\s+3|[Dd]river\s+[Cc]lusters?)(.*?)(?:Section\s+4|Asset\s+Profile)',
        html, re.DOTALL | re.IGNORECASE
    )
    if driver_section:
        cluster_matches = re.findall(
            r'<(?:strong|b)>\s*([^<]+?)\s*(?::</?\s*(?:/strong>|/b>))',
            driver_section.group(1), re.IGNORECASE
        )
        for cluster_name in cluster_matches[:6]:
            clean_name = re.sub(r'<[^>]+>', '', cluster_name).strip().rstrip(':')
            if len(clean_name) > 3:
                driver_clusters.append({"cluster": clean_name, "weight": 0})

    # --- Assemble signal_pack ---
    signal_pack = {
        "metadata": {
            "primary_asset_name": primary_asset_name,
            "report_id": report_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "decay_window_hours": pas.get("decay_window_estimate_hours", 24),
            "source": "html_parse",
        },
        "primary_asset_signal": {
            "primary_asset_name": primary_asset_name,
            "direction_bias": pas.get("direction_bias", "Unknown"),
            "pressure_index": pas.get("pressure_index", 0),
            "acceleration_delta": pas.get("acceleration_delta", 0),
            "signal_strength": pas.get("signal_strength", "Moderate"),
            "decay_window_estimate_hours": pas.get("decay_window_estimate_hours", 24),
            "top_driver_clusters": driver_clusters,
        },
        "cascade_map": cascade_map,
        "candidate_trades": candidate_trades,
    }

    logger.info(
        "Parsed HTML report: {} | PI={} Dir={} Accel={} Str={} | {} cascades, {} trades".format(
            primary_asset_name,
            pas.get("pressure_index"),
            pas.get("direction_bias"),
            pas.get("acceleration_delta"),
            pas.get("signal_strength"),
            len(cascade_map),
            len(candidate_trades),
        ))

    return signal_pack


def parse_8a_markdown(markdown_text, meta=None):
    """Parse signal data from 8A workflow output report_markdown.

    The markdown contains structured sections:
    - Section 2: Primary Asset Signal Dashboard (table with | Metric | Value |)
    - Section 3: Driver Clusters (bullet list)
    - Section 5: Cascade Map (bullet list)
    - Section 6: Candidate Trades (bullet list)

    Returns a signal_pack dict or None.
    """
    if not markdown_text:
        return None

    meta = meta or {}
    pas = {}

    # --- Parse Primary Asset Signal Dashboard (Section 2 table) ---
    # Look for markdown table rows: | Metric | Value |
    table_rows = re.findall(
        r'\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|',
        markdown_text
    )
    field_map = {
        "pressure index": "pressure_index",
        "direction bias": "direction_bias",
        "acceleration delta": "acceleration_delta",
        "signal strength": "signal_strength",
        "decay window": "decay_window_estimate_hours",
        "entity name": "primary_asset_name",
    }

    for metric_raw, value_raw in table_rows:
        metric = metric_raw.strip().lower()
        value = value_raw.strip()
        # Skip header rows
        if metric in ("metric", "field", "---", "--------", "-------"):
            continue
        if value.startswith("---"):
            continue

        for key, field in field_map.items():
            if key in metric:
                if field in ("pressure_index", "acceleration_delta"):
                    try:
                        pas[field] = float(value.replace('+', ''))
                    except ValueError:
                        pas[field] = value
                elif field == "decay_window_estimate_hours":
                    m = re.search(r'(\d+)', value)
                    if m:
                        pas[field] = float(m.group(1))
                else:
                    pas[field] = value
                break

    if not pas.get("pressure_index"):
        # Fallback: extract from prose text
        pi_match = re.search(r'Pressure\s+Index\s+(\d+(?:\.\d+)?)', markdown_text)
        if pi_match:
            pas["pressure_index"] = float(pi_match.group(1))

        dir_match = re.search(r'Direction\s+Bias\s+(UP|DOWN)', markdown_text, re.IGNORECASE)
        if dir_match:
            pas["direction_bias"] = dir_match.group(1)

        accel_match = re.search(r'Acceleration\s+Delta\s+([+-]?\d+(?:\.\d+)?)', markdown_text)
        if accel_match:
            pas["acceleration_delta"] = float(accel_match.group(1))

        str_match = re.search(r'[Ss]ignal [Ss]trength[^.]*?(High|Moderate|Low)', markdown_text)
        if str_match:
            pas["signal_strength"] = str_match.group(1).capitalize()

        decay_match = re.search(r'decay\s+window[^.]*?(\d+)[\s-]*hour', markdown_text, re.IGNORECASE)
        if decay_match:
            pas["decay_window_estimate_hours"] = float(decay_match.group(1))

    if not pas.get("pressure_index"):
        logger.warning("Could not extract primary asset signal from 8A markdown")
        return None

    # --- Extract primary asset name from title line or meta ---
    primary_asset_name = None
    title_match = re.search(
        r'NARRATIVE\s+ASSET\s+SIGNAL\s+BRIEF\s*(?::|---|\u2014)\s*(.+?)\s*(?:---|---|\u2014)\s*\d{4}',
        markdown_text, re.IGNORECASE
    )
    if title_match:
        primary_asset_name = title_match.group(1).strip()
    if not primary_asset_name:
        primary_asset_name = (
            pas.get("primary_asset_name") or
            meta.get("target_entity") or
            "Unknown"
        )

    # --- Extract report code from title [XXXX] ---
    report_id_match = re.search(r'\[([A-Z0-9]{4})\]', markdown_text)
    report_id = report_id_match.group(1) if report_id_match else meta.get("report_code")

    # --- Parse Cascade Map (Section 5) ---
    cascade_map = []
    cascade_section = re.search(
        r'(?:##\s*)?Section\s+5\s*(?:---|:|\u2014)?\s*Cascade\s+Map(.*?)(?:(?:##\s*)?Section\s+6|$)',
        markdown_text, re.DOTALL | re.IGNORECASE
    )
    if cascade_section:
        cascade_text = cascade_section.group(1)
        # Match: * **Category:** expected DIRECTION. Instruments: XLE, OIH.
        entries = re.findall(
            r'\*\*\s*([^*]+?)\s*:?\s*\*\*\s*:?\s*expected\s+(UP|DOWN)\.'
            r'\s*Instruments?:\s*([^.\n]+)',
            cascade_text, re.IGNORECASE
        )
        for category_raw, direction, instruments_raw in entries:
            category = category_raw.strip().rstrip(':')
            instruments = [t.strip() for t in instruments_raw.strip().split(',')
                           if t.strip()]
            cascade_entry = {
                "exposure_category": category.lower(),
                "expected_direction": direction.upper(),
                "magnitude": "Moderate",
                "lag_hours": 0,
            }
            if instruments:
                cascade_entry["instruments"] = instruments
            cascade_map.append(cascade_entry)

    # --- Parse Candidate Trades (Section 6) ---
    candidate_trades = []
    trades_section = re.search(
        r'(?:##\s*)?Section\s+6\s*(?:---|:|\u2014)?\s*Candidate\s+Trades(.*?)(?:(?:##\s*)?Section\s+7|$)',
        markdown_text, re.DOTALL | re.IGNORECASE
    )
    if trades_section:
        trades_text = trades_section.group(1)
        # Match: * **TICKER:** If ... expected to move DIRECTION ...
        trade_matches = re.findall(
            r'\*\*\s*([A-Z][A-Z0-9 ]{0,15})\s*:?\s*\*\*\s*:?\s*'
            r'If[^.]*?expected\s+to\s+move\s+(up|down)',
            trades_text, re.IGNORECASE
        )
        for instr, direction in trade_matches:
            instr = instr.strip()
            candidate_trades.append({
                "instrument": instr,
                "direction": direction.upper(),
            })

    # --- Parse Driver Clusters (Section 3) ---
    driver_clusters = []
    driver_section = re.search(
        r'(?:##\s*)?Section\s+3\s*(?:---|:|\u2014)?\s*Driver\s+Clusters(.*?)(?:(?:##\s*)?Section\s+4|$)',
        markdown_text, re.DOTALL | re.IGNORECASE
    )
    if driver_section:
        cluster_names = re.findall(
            r'\*\*\s*([^*]+?)\s*:?\s*\*\*\s*:',
            driver_section.group(1)
        )
        for i, name in enumerate(cluster_names[:6]):
            clean_name = name.strip().rstrip(':')
            if len(clean_name) > 3:
                driver_clusters.append({"cluster": clean_name, "weight": 0})

    # --- Assemble signal_pack ---
    signal_pack = {
        "metadata": {
            "primary_asset_name": primary_asset_name,
            "report_id": report_id,
            "generated_at": meta.get("analysis_timestamp",
                                     datetime.now(timezone.utc).isoformat()),
            "decay_window_hours": pas.get("decay_window_estimate_hours", 24),
            "source": "8a_markdown",
        },
        "primary_asset_signal": {
            "primary_asset_name": primary_asset_name,
            "direction_bias": pas.get("direction_bias", "Unknown"),
            "pressure_index": pas.get("pressure_index", 0),
            "acceleration_delta": pas.get("acceleration_delta", 0),
            "signal_strength": pas.get("signal_strength", "Moderate"),
            "decay_window_estimate_hours": pas.get("decay_window_estimate_hours", 24),
            "top_driver_clusters": driver_clusters,
        },
        "cascade_map": cascade_map,
        "candidate_trades": candidate_trades,
    }

    logger.info(
        "Parsed 8A markdown: {} [{}] | PI={} Dir={} Accel={} Str={} | {} cascades, {} trades".format(
            primary_asset_name,
            report_id,
            pas.get("pressure_index"),
            pas.get("direction_bias"),
            pas.get("acceleration_delta"),
            pas.get("signal_strength"),
            len(cascade_map),
            len(candidate_trades),
        ))

    return signal_pack


def normalise_signal_pack(raw):
    """Normalise a signal_pack JSON object into our standard structure.
    Handles both the full signal_pack wrapper and direct signal objects,
    including 8A workflow output with embedded report_markdown.
    """
    if not raw or not isinstance(raw, dict):
        return None

    # If the raw object IS a signal_pack (has metadata, primary_asset_signal, etc.)
    if "primary_asset_signal" in raw and "cascade_map" in raw:
        return raw

    # If wrapped in a signal_pack key
    if "signal_pack" in raw:
        return raw["signal_pack"]

    # If it's an 8A workflow output — build signal_pack from structured fields
    for key in ["workflow_8A_output", "workflow8a_output"]:
        if key in raw and isinstance(raw[key], dict):
            inner = raw[key]
            meta = inner.get("meta", {})

            # PREFER top-level structured cascade_map (complete) over
            # the embedded signal_pack.cascade_map (often incomplete)
            top_cascade = inner.get("cascade_map", [])
            top_pas = inner.get("primary_asset_signal", {})
            embedded_sp = inner.get("signal_pack", {}) or {}
            embedded_pas = embedded_sp.get("primary_asset_signal", {}) or {}

            # Build the best primary_asset_signal from available data
            # Top-level and embedded use slightly different field names
            pas = {
                "primary_asset_name": (
                    meta.get("target_entity") or
                    embedded_sp.get("metadata", {}).get("primary_asset_name") or
                    "Unknown"
                ),
                "pressure_index": (
                    top_pas.get("pressure_index") or
                    top_pas.get("pressure_index_0_to_100") or
                    embedded_pas.get("pressure_index") or
                    embedded_pas.get("pressure_index_0_to_100") or
                    0
                ),
                "direction_bias": (
                    top_pas.get("direction_bias") or
                    embedded_pas.get("direction_bias") or
                    "Unknown"
                ),
                "acceleration_delta": (
                    top_pas.get("acceleration_delta") or
                    embedded_pas.get("acceleration_delta") or
                    0
                ),
                "signal_strength": (
                    top_pas.get("signal_strength") or
                    embedded_pas.get("signal_strength") or
                    "Moderate"
                ),
                "decay_window_estimate_hours": (
                    top_pas.get("decay_window_estimate_hours") or
                    embedded_pas.get("decay_window_estimate_hours") or
                    24
                ),
                "top_driver_clusters": (
                    top_pas.get("top_driver_clusters") or
                    embedded_pas.get("top_driver_clusters") or
                    []
                ),
            }

            # Normalise cascade_map field names (candidate_instruments -> instruments)
            cascade_map = []
            source_cascade = top_cascade if top_cascade else embedded_sp.get("cascade_map", [])
            for entry in source_cascade:
                cascade_entry = {
                    "exposure_category": entry.get("exposure_category", ""),
                    "expected_direction": entry.get("expected_direction", ""),
                    "magnitude": entry.get("magnitude", "Moderate"),
                    "lag_hours": entry.get("lag_hours", 0),
                }
                # Handle both "instruments" and "candidate_instruments"
                instruments = (
                    entry.get("instruments") or
                    entry.get("candidate_instruments") or
                    []
                )
                if instruments:
                    cascade_entry["instruments"] = instruments
                cascade_map.append(cascade_entry)

            # Normalise candidate_trades
            candidate_trades = (
                inner.get("candidate_trades") or
                embedded_sp.get("candidate_trades") or
                []
            )

            # Extract report_id
            report_id = meta.get("report_code")
            if not report_id and "report_markdown" in inner:
                rid_match = re.search(r'\[([A-Z0-9]{4})\]', inner["report_markdown"])
                if rid_match:
                    report_id = rid_match.group(1)

            # If we have usable data, build signal_pack
            if cascade_map or pas.get("pressure_index"):
                signal_pack = {
                    "metadata": {
                        "primary_asset_name": pas["primary_asset_name"],
                        "report_id": report_id,
                        "generated_at": meta.get("analysis_timestamp",
                                                 datetime.now(timezone.utc).isoformat()),
                        "decay_window_hours": pas["decay_window_estimate_hours"],
                        "source": "8a_structured",
                    },
                    "primary_asset_signal": pas,
                    "cascade_map": cascade_map,
                    "candidate_trades": candidate_trades,
                }
                logger.info(
                    "Built signal_pack from 8A structured data: {} | PI={} Dir={} | {} cascades".format(
                        pas["primary_asset_name"], pas["pressure_index"],
                        pas["direction_bias"], len(cascade_map)))
                return signal_pack

            # Fallback: parse the report_markdown
            if "report_markdown" in inner:
                return parse_8a_markdown(inner["report_markdown"], meta=meta)

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
            # Accept items that match our prefix OR contain "Narrative Asset Signal"
            is_nas = (
                (REPORT_TITLE_PREFIX and REPORT_TITLE_PREFIX.lower() in title.lower()) or
                "narrative asset signal" in title.lower() or
                "signal brief" in title.lower()
            )
            if not is_nas:
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

            # Method 1: Try extracting signal_pack JSON from description
            signal_pack = None
            raw = extract_signal_pack(item.get("description", ""))
            if raw:
                signal_pack = normalise_signal_pack(raw)

            # Method 2: If no JSON found, fetch and parse the HTML report page
            if not signal_pack and item.get("link"):
                logger.info("No JSON in RSS, parsing HTML report: {}".format(
                    item["link"][:80]))
                signal_pack = parse_html_report(item["link"])

            if not signal_pack:
                logger.warning("Could not extract signal from: {}".format(title[:60]))
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
