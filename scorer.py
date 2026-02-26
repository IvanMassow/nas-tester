"""
NAS Signal Tester - Scorer
Computes horizon scores, excursions, lead time, and the Advantage Clock.
This is the core commercial proof engine.
"""
import logging
from datetime import datetime, timezone, timedelta

from db import get_conn
from config import HORIZONS, MIN_MOVE_PCT

logger = logging.getLogger("nas.scorer")


def score_position_at_horizon(position, horizon_hours, min_move_pct=None):
    """Score a single position at a specific horizon.
    Returns a score dict or None if data not yet available.
    """
    if min_move_pct is None:
        min_move_pct = MIN_MOVE_PCT

    entry_price = position["entry_price"]
    if not entry_price or entry_price <= 0:
        return None

    expected_direction = position["expected_direction"]
    opened_at = datetime.fromisoformat(position["opened_at"])
    if opened_at.tzinfo is None:
        opened_at = opened_at.replace(tzinfo=timezone.utc)

    target_time = opened_at + timedelta(hours=horizon_hours)

    # Find the closest price sample to the target time (within 60 min tolerance)
    conn = get_conn()
    earliest = (target_time - timedelta(minutes=60)).isoformat()
    latest = (target_time + timedelta(minutes=60)).isoformat()

    sample = conn.execute("""
        SELECT * FROM price_samples
        WHERE position_id = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY ABS(julianday(timestamp) - julianday(?))
        LIMIT 1
    """, (position["id"], earliest, latest, target_time.isoformat())).fetchone()
    conn.close()

    if not sample or not sample["price"]:
        return None

    return_pct = (sample["price"] - entry_price) / entry_price * 100

    if expected_direction == "UP":
        passed = return_pct > min_move_pct
    elif expected_direction == "DOWN":
        passed = return_pct < -min_move_pct
    else:
        passed = abs(return_pct) > min_move_pct

    return {
        "horizon_hours": horizon_hours,
        "return_pct": round(return_pct, 4),
        "passed": 1 if passed else 0,
        "sample_price": sample["price"],
        "entry_price": entry_price,
        "min_move_pct": min_move_pct,
        "expected_direction": expected_direction,
    }


def compute_excursions(position_id, entry_price, expected_direction):
    """Compute max favorable/adverse excursion from all price samples."""
    if not entry_price or entry_price <= 0:
        return 0.0, 0.0

    conn = get_conn()
    samples = conn.execute("""
        SELECT price FROM price_samples
        WHERE position_id = ? AND price IS NOT NULL
        ORDER BY timestamp ASC
    """, (position_id,)).fetchall()
    conn.close()

    if not samples:
        return 0.0, 0.0

    max_favorable = 0.0
    max_adverse = 0.0

    for s in samples:
        ret = (s["price"] - entry_price) / entry_price * 100
        if expected_direction == "UP":
            if ret > max_favorable:
                max_favorable = ret
            if ret < max_adverse:
                max_adverse = ret
        else:  # DOWN — favorable is negative return
            if -ret > max_favorable:
                max_favorable = -ret
            if -ret < max_adverse:
                max_adverse = -ret

    return round(max_favorable, 4), round(max_adverse, 4)


def compute_lead_time(position_id, entry_price, expected_direction, opened_at_str,
                      min_move_pct=None):
    """Compute the Advantage Clock: lead time and key timestamps.

    T_belief = When signal_pack arrived (position opened_at)
    T_reality = First price sample where market crossed min_move in expected direction
    T_peak = When the favorable move peaked
    lead_time = T_reality - T_belief (hours)
    """
    if min_move_pct is None:
        min_move_pct = MIN_MOVE_PCT

    if not entry_price or entry_price <= 0:
        return None

    opened_at = datetime.fromisoformat(opened_at_str)
    if opened_at.tzinfo is None:
        opened_at = opened_at.replace(tzinfo=timezone.utc)

    conn = get_conn()
    samples = conn.execute("""
        SELECT price, timestamp FROM price_samples
        WHERE position_id = ? AND price IS NOT NULL
        ORDER BY timestamp ASC
    """, (position_id,)).fetchall()
    conn.close()

    if not samples:
        return None

    t_belief = opened_at
    t_reality = None
    t_peak = None
    peak_return = 0.0

    for s in samples:
        ret = (s["price"] - entry_price) / entry_price * 100
        sample_time = datetime.fromisoformat(s["timestamp"])
        if sample_time.tzinfo is None:
            sample_time = sample_time.replace(tzinfo=timezone.utc)

        # Check for reality moment (first crossing of min_move threshold)
        if t_reality is None:
            if expected_direction == "UP" and ret > min_move_pct:
                t_reality = sample_time
            elif expected_direction == "DOWN" and ret < -min_move_pct:
                t_reality = sample_time

        # Track peak favorable move
        if expected_direction == "UP":
            if ret > peak_return:
                peak_return = ret
                t_peak = sample_time
        else:  # DOWN
            if -ret > peak_return:
                peak_return = -ret
                t_peak = sample_time

    lead_time_hours = None
    if t_reality:
        lead_time_hours = (t_reality - t_belief).total_seconds() / 3600

    # Determine advantage_state
    now = datetime.now(timezone.utc)
    hours_elapsed = (now - t_belief).total_seconds() / 3600

    if t_reality is None:
        # Market hasn't confirmed yet
        if hours_elapsed > 72:
            advantage_state = "EXPIRED"
        else:
            advantage_state = "AHEAD"  # Still in the advantage window
    elif t_peak and t_peak > t_reality:
        advantage_state = "PEAKED"
    else:
        advantage_state = "CONFIRMED"

    return {
        "t_belief": t_belief.isoformat(),
        "t_reality": t_reality.isoformat() if t_reality else None,
        "t_peak": t_peak.isoformat() if t_peak else None,
        "lead_time_hours": round(lead_time_hours, 2) if lead_time_hours is not None else None,
        "peak_return_pct": round(peak_return, 2),
        "advantage_state": advantage_state,
        "hours_elapsed": round(hours_elapsed, 2),
    }


def score_all():
    """Score all positions that have entry prices and price samples.
    Updates both position_scores table and denormalised fields on positions.
    """
    conn = get_conn()
    positions = conn.execute("""
        SELECT * FROM positions
        WHERE entry_price IS NOT NULL AND entry_price > 0
    """).fetchall()
    conn.close()

    if not positions:
        logger.info("No positions with entry prices to score")
        return 0

    scored_count = 0
    for pos in positions:
        pos_dict = dict(pos)
        pid = pos["id"]

        # Score at each horizon
        horizon_results = {}
        for h in HORIZONS:
            result = score_position_at_horizon(pos_dict, h)
            if result:
                horizon_results[h] = result
                # Upsert into position_scores
                conn = get_conn()
                conn.execute("""
                    INSERT OR REPLACE INTO position_scores
                    (position_id, horizon_hours, scored_at, return_pct,
                     expected_direction, min_move_pct, passed, sample_price, entry_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pid, h, datetime.now(timezone.utc).isoformat(),
                    result["return_pct"], result["expected_direction"],
                    result["min_move_pct"], result["passed"],
                    result["sample_price"], result["entry_price"]
                ))
                conn.commit()
                conn.close()

        # Compute excursions
        max_fav, max_adv = compute_excursions(
            pid, pos["entry_price"], pos["expected_direction"])

        # Compute lead time / advantage clock
        lead = compute_lead_time(
            pid, pos["entry_price"], pos["expected_direction"], pos["opened_at"])

        # Find earliest hit horizon
        earliest_hit = None
        for h in sorted(HORIZONS):
            if h in horizon_results and horizon_results[h]["passed"]:
                earliest_hit = h
                break

        # Update denormalised fields on position
        conn = get_conn()
        conn.execute("""
            UPDATE positions SET
                hit_6h = ?,
                hit_12h = ?,
                hit_24h = ?,
                hit_72h = ?,
                earliest_hit_hours = ?,
                max_favorable_excursion = ?,
                max_adverse_excursion = ?,
                t_belief = ?,
                t_reality = ?,
                t_peak = ?,
                lead_time_hours = ?,
                advantage_state = ?
            WHERE id = ?
        """, (
            horizon_results.get(6, {}).get("passed"),
            horizon_results.get(12, {}).get("passed"),
            horizon_results.get(24, {}).get("passed"),
            horizon_results.get(72, {}).get("passed"),
            earliest_hit,
            max_fav, max_adv,
            lead["t_belief"] if lead else pos["opened_at"],
            lead["t_reality"] if lead else None,
            lead["t_peak"] if lead else None,
            lead["lead_time_hours"] if lead else None,
            lead["advantage_state"] if lead else "AHEAD",
            pid
        ))
        conn.commit()
        conn.close()
        scored_count += 1

    logger.info("Scored {} positions".format(scored_count))
    return scored_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    from db import init_db
    init_db()
    score_all()
