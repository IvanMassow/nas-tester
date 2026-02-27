"""
NAS Signal Tester - Position Manager
Opens, reinforces, flips, and closes paper positions from cascade map entries.
Deterministic rules — no LLM, no human in the loop.
"""
import json
import logging
from datetime import datetime, timezone, timedelta

from db import get_conn
from resolver import resolve_proxy, resolve_all_cascade
from config import (
    DEFAULT_DECAY_WINDOW_HOURS, MAX_MONITORING_HOURS,
    ACCEL_REFRESH_THRESHOLD
)

logger = logging.getLogger("nas.position_manager")


def open_positions_from_signal(signal_id, cascade_map, candidate_trades=None,
                                 decay_window_hours=None, signal_strength=None,
                                 primary_asset_signal=None):
    """Process a cascade map and open/reinforce/flip positions.
    Returns count of positions opened or modified.

    When cascade_map is empty (e.g. semiconductor ETFs where the primary asset
    IS the tradeable instrument), creates a synthetic cascade entry from
    the primary_asset_signal so we still track the position.
    """
    if not cascade_map and primary_asset_signal:
        # No cascade — the primary asset IS the position
        # Try to extract ticker from asset name e.g. "VanEck Semiconductor ETF (SMH)"
        import re
        asset_name = primary_asset_signal.get("primary_asset_name", "")
        direction = primary_asset_signal.get("direction_bias", "UP")
        ticker_match = re.search(r'\(([A-Z]{2,5})\)', asset_name)
        ticker = ticker_match.group(1) if ticker_match else None

        if ticker:
            cascade_map = [{
                "exposure_category": "primary_asset",
                "expected_direction": direction,
                "magnitude": "Moderate",
                "lag_hours": 0,
                "instruments": [ticker],
            }]
            logger.info("Signal {}: no cascade — created synthetic entry for {} {}".format(
                signal_id, ticker, direction))
        else:
            logger.info("Signal {}: no cascade and no ticker in asset name '{}'".format(
                signal_id, asset_name))
            return 0

    if not cascade_map:
        logger.info("Signal {}: empty cascade map, no positions to open".format(signal_id))
        return 0

    decay = decay_window_hours or DEFAULT_DECAY_WINDOW_HOURS
    now = datetime.now(timezone.utc)
    monitoring_until = now + timedelta(hours=min(decay * 2, MAX_MONITORING_HOURS))

    # Build candidate trade lookup for trigger/invalidation conditions
    trade_lookup = {}
    if candidate_trades:
        for ct in candidate_trades:
            instrument = ct.get("instrument", "")
            if instrument:
                trade_lookup[instrument.lower()] = ct

    # Resolve all cascade entries to proxy tickers
    resolved = resolve_all_cascade(cascade_map)

    count = 0
    for entry, ticker, method in resolved:
        if not ticker:
            category = entry.get("exposure_category", "unknown")
            logger.warning("Signal {}: could not resolve ticker for '{}'".format(
                signal_id, category))
            continue

        category = entry.get("exposure_category", "")
        expected_dir = entry.get("expected_direction", "UP")
        if expected_dir not in ("UP", "DOWN"):
            expected_dir = "UP" if expected_dir.upper() in ("UP", "POSITIVE", "LONG") else "DOWN"

        # Check for trigger/invalidation conditions from candidate_trades
        trigger_cond = None
        invalidation_cond = None
        ct = trade_lookup.get(ticker.lower()) or trade_lookup.get(category.lower())
        if ct:
            trigger_cond = ct.get("trigger_condition")
            invalidation_cond = ct.get("invalidation_condition")

        # Apply position management rules
        result = _manage_position(
            signal_id=signal_id,
            cascade_category=category,
            proxy_ticker=ticker,
            expected_direction=expected_dir,
            resolve_method=method,
            decay_window_hours=decay,
            monitoring_until=monitoring_until.isoformat(),
            trigger_condition=trigger_cond,
            invalidation_condition=invalidation_cond,
        )
        if result:
            count += 1

    logger.info("Signal {}: {} positions opened/modified from {} cascade entries".format(
        signal_id, count, len(cascade_map)))
    return count


def _manage_position(signal_id, cascade_category, proxy_ticker, expected_direction,
                     resolve_method, decay_window_hours, monitoring_until,
                     trigger_condition=None, invalidation_condition=None):
    """Apply position management rules for a single cascade entry.
    Returns True if a position was created or modified.

    Rules:
    1. Same ticker + same direction -> Reinforce (keep T0, bump count)
    2. Same ticker + direction flip -> Close old, open new
    3. Same ticker + same direction + longer decay -> Extend monitoring
    """
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()

    # Check for existing active position on this ticker
    existing = conn.execute("""
        SELECT * FROM positions
        WHERE proxy_ticker = ? AND is_active = 1 AND state IN ('ACTIVE', 'REINFORCED')
        ORDER BY opened_at DESC LIMIT 1
    """, (proxy_ticker,)).fetchone()

    if existing:
        if existing["expected_direction"] == expected_direction:
            # Rule 1: Same direction -> Reinforce
            reinforced_by = json.loads(existing["reinforced_by"] or "[]")
            if signal_id not in reinforced_by:
                reinforced_by.append(signal_id)

            # Optionally extend monitoring window
            new_monitoring = monitoring_until
            if existing["monitoring_until"] and existing["monitoring_until"] > monitoring_until:
                new_monitoring = existing["monitoring_until"]

            conn.execute("""
                UPDATE positions SET
                    state = 'REINFORCED',
                    reinforced_count = reinforced_count + 1,
                    reinforced_by = ?,
                    last_reinforced_at = ?,
                    monitoring_until = CASE
                        WHEN monitoring_until < ? THEN ?
                        ELSE monitoring_until
                    END
                WHERE id = ?
            """, (json.dumps(reinforced_by), now,
                  new_monitoring, new_monitoring,
                  existing["id"]))
            conn.commit()
            conn.close()
            logger.info("  Reinforced {} {} ({}) - now {} reinforcements".format(
                proxy_ticker, expected_direction, cascade_category,
                existing["reinforced_count"] + 1))
            return True
        else:
            # Rule 2: Direction flip -> Close old, open new
            conn.execute("""
                UPDATE positions SET
                    state = 'CLOSED',
                    is_active = 0,
                    closed_at = ?,
                    close_reason = 'direction_flip_by_{}'
                WHERE id = ?
            """.format(signal_id), (now, existing["id"]))
            conn.commit()
            logger.info("  Closed {} {} (direction flip to {})".format(
                proxy_ticker, existing["expected_direction"], expected_direction))
            # Fall through to open new position below

    # Open new position
    conn.execute("""
        INSERT INTO positions
        (signal_id, cascade_category, expected_direction, proxy_ticker, resolve_method,
         state, opened_at, decay_window_hours, monitoring_until,
         trigger_condition, invalidation_condition,
         t_belief, advantage_state, is_active)
        VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, 'AHEAD', 1)
    """, (
        signal_id, cascade_category, expected_direction, proxy_ticker, resolve_method,
        now, decay_window_hours, monitoring_until,
        trigger_condition, invalidation_condition,
        now,  # t_belief = opened_at
    ))
    conn.commit()
    conn.close()

    logger.info("  Opened {} {} {} ({})".format(
        proxy_ticker, expected_direction, cascade_category, resolve_method))
    return True


def close_expired_positions():
    """Close positions past their monitoring window."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    result = conn.execute("""
        UPDATE positions SET
            state = 'EXPIRED',
            is_active = 0,
            closed_at = ?,
            close_reason = 'monitoring_window_expired'
        WHERE is_active = 1 AND monitoring_until <= ?
    """, (now, now))
    if result.rowcount > 0:
        logger.info("Expired {} positions past monitoring window".format(result.rowcount))
    conn.commit()
    conn.close()
    return result.rowcount


def get_active_positions():
    """Get all active positions."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT p.*, s.primary_asset, s.signal_strength, s.pressure_index,
               s.acceleration_delta, s.direction_bias
        FROM positions p
        JOIN signals s ON p.signal_id = s.signal_id
        WHERE p.is_active = 1
        ORDER BY p.opened_at DESC
    """).fetchall()
    conn.close()
    return rows


def get_all_positions():
    """Get all positions (active and closed) for analytics."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT p.*, s.primary_asset, s.signal_strength, s.pressure_index,
               s.acceleration_delta, s.direction_bias
        FROM positions p
        JOIN signals s ON p.signal_id = s.signal_id
        ORDER BY p.opened_at DESC
    """).fetchall()
    conn.close()
    return rows


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from db import init_db
    init_db()

    positions = get_active_positions()
    print("Active positions: {}".format(len(positions)))
    for p in positions:
        print("  {} {} {} (state={}, since={})".format(
            p["proxy_ticker"], p["expected_direction"], p["cascade_category"],
            p["state"], p["opened_at"][:16]))
