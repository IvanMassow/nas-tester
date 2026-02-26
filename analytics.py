"""
NAS Signal Tester - Analytics
Generates portfolio-level metrics for reporting and the Claude briefing.
"""
import json
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from db import get_conn
from config import HORIZONS, MIN_MOVE_PCT, STRENGTH_ORDER, TRADEABLE_MIN_STRENGTH

logger = logging.getLogger("nas.analytics")


def generate_analytics():
    """Generate the full analytics payload for report generation.
    Returns a dict with all metrics needed by report_html.py.
    """
    conn = get_conn()

    # All signals
    signals = conn.execute("SELECT * FROM signals ORDER BY ingested_at DESC").fetchall()

    # All positions with signal data
    positions = conn.execute("""
        SELECT p.*, s.primary_asset, s.signal_strength, s.pressure_index,
               s.acceleration_delta, s.direction_bias
        FROM positions p
        JOIN signals s ON p.signal_id = s.signal_id
        ORDER BY p.opened_at DESC
    """).fetchall()

    # All scores
    scores = conn.execute("""
        SELECT ps.*, p.cascade_category, p.proxy_ticker, p.expected_direction,
               p.signal_id, s.signal_strength as sig_strength, s.pressure_index as sig_pressure
        FROM position_scores ps
        JOIN positions p ON ps.position_id = p.id
        JOIN signals s ON p.signal_id = s.signal_id
    """).fetchall()

    conn.close()

    # Convert to dicts for processing
    signals = [dict(s) for s in signals]
    positions = [dict(p) for p in positions]
    scores = [dict(s) for s in scores]

    # --- Summary stats ---
    total_signals = len(signals)
    total_positions = len(positions)
    active_positions = sum(1 for p in positions if p["is_active"])
    closed_positions = sum(1 for p in positions if not p["is_active"])

    # Positions with entry prices (scoreable)
    scoreable = [p for p in positions if p["entry_price"] and p["entry_price"] > 0]

    # --- Hit rates by horizon ---
    hit_rates = {}
    for h in HORIZONS:
        key = "hit_{}h".format(h)
        hits = [p for p in scoreable if p.get(key) is not None]
        if hits:
            rate = sum(1 for p in hits if p[key] == 1) / len(hits) * 100
            hit_rates[h] = {
                "rate": round(rate, 1),
                "hits": sum(1 for p in hits if p[key] == 1),
                "total": len(hits),
            }
        else:
            hit_rates[h] = {"rate": None, "hits": 0, "total": 0}

    # --- Lead time stats ---
    lead_times = [p["lead_time_hours"] for p in scoreable
                  if p.get("lead_time_hours") is not None]
    avg_lead_time = round(sum(lead_times) / len(lead_times), 1) if lead_times else None

    # Advantage distribution
    advantage_dist = {"0-2h": 0, "2-6h": 0, "6-12h": 0, "12-24h": 0, "24h+": 0}
    for lt in lead_times:
        if lt <= 2:
            advantage_dist["0-2h"] += 1
        elif lt <= 6:
            advantage_dist["2-6h"] += 1
        elif lt <= 12:
            advantage_dist["6-12h"] += 1
        elif lt <= 24:
            advantage_dist["12-24h"] += 1
        else:
            advantage_dist["24h+"] += 1

    # Advantage states
    advantage_states = defaultdict(int)
    for p in scoreable:
        state = p.get("advantage_state", "AHEAD")
        advantage_states[state] += 1

    # --- Cascade category leaderboard ---
    category_stats = defaultdict(lambda: {
        "positions": 0, "hits_24h": 0, "scored_24h": 0,
        "avg_return_right": [], "avg_return_wrong": [],
        "avg_lead_time": [], "tickers": set(),
    })
    for p in scoreable:
        cat = p.get("cascade_category", "unknown")
        stats = category_stats[cat]
        stats["positions"] += 1
        if p["proxy_ticker"]:
            stats["tickers"].add(p["proxy_ticker"])
        if p.get("hit_24h") is not None:
            stats["scored_24h"] += 1
            if p["hit_24h"] == 1:
                stats["hits_24h"] += 1
        if p.get("lead_time_hours") is not None:
            stats["avg_lead_time"].append(p["lead_time_hours"])

        # Compute return for right/wrong
        for h in HORIZONS:
            key = "hit_{}h".format(h)
            if p.get(key) is not None:
                # Get the actual return from scores
                score = next((s for s in scores
                              if s["position_id"] == p["id"] and s["horizon_hours"] == h), None)
                if score and score["return_pct"] is not None:
                    if p[key] == 1:
                        stats["avg_return_right"].append(abs(score["return_pct"]))
                    else:
                        stats["avg_return_wrong"].append(abs(score["return_pct"]))

    # Build leaderboard
    leaderboard = []
    for cat, stats in category_stats.items():
        hit_rate = (stats["hits_24h"] / stats["scored_24h"] * 100
                    if stats["scored_24h"] > 0 else None)
        avg_right = (round(sum(stats["avg_return_right"]) / len(stats["avg_return_right"]), 2)
                     if stats["avg_return_right"] else None)
        avg_wrong = (round(sum(stats["avg_return_wrong"]) / len(stats["avg_return_wrong"]), 2)
                     if stats["avg_return_wrong"] else None)
        avg_lead = (round(sum(stats["avg_lead_time"]) / len(stats["avg_lead_time"]), 1)
                    if stats["avg_lead_time"] else None)
        leaderboard.append({
            "category": cat,
            "positions": stats["positions"],
            "hit_rate_24h": hit_rate,
            "avg_return_when_right": avg_right,
            "avg_return_when_wrong": avg_wrong,
            "avg_lead_time_hours": avg_lead,
            "tickers": sorted(stats["tickers"]),
        })
    leaderboard.sort(key=lambda x: (x["hit_rate_24h"] or 0), reverse=True)

    # --- Signal strength vs accuracy ---
    strength_stats = {}
    for strength in ["High", "Moderate", "Low"]:
        s_positions = [p for p in scoreable if p.get("signal_strength") == strength]
        s_hit_rates = {}
        for h in HORIZONS:
            key = "hit_{}h".format(h)
            scored = [p for p in s_positions if p.get(key) is not None]
            if scored:
                rate = sum(1 for p in scored if p[key] == 1) / len(scored) * 100
                s_hit_rates[h] = round(rate, 1)
            else:
                s_hit_rates[h] = None
        strength_stats[strength] = {
            "count": len(s_positions),
            "hit_rates": s_hit_rates,
        }

    # --- Per-primary-asset breakdown ---
    asset_stats = defaultdict(lambda: {
        "signals": 0, "positions": 0, "hit_rates": {},
        "avg_lead_time": None, "best_category": None,
    })
    for s in signals:
        asset = s.get("primary_asset", "Unknown")
        asset_stats[asset]["signals"] += 1

    for p in scoreable:
        asset = p.get("primary_asset", "Unknown")
        asset_stats[asset]["positions"] += 1

    asset_breakdown = []
    for asset, stats in asset_stats.items():
        a_positions = [p for p in scoreable if p.get("primary_asset") == asset]
        a_hit_rates = {}
        for h in HORIZONS:
            key = "hit_{}h".format(h)
            scored = [p for p in a_positions if p.get(key) is not None]
            if scored:
                rate = sum(1 for p in scored if p[key] == 1) / len(scored) * 100
                a_hit_rates[h] = round(rate, 1)
            else:
                a_hit_rates[h] = None
        a_leads = [p["lead_time_hours"] for p in a_positions
                   if p.get("lead_time_hours") is not None]
        asset_breakdown.append({
            "asset": asset,
            "signals": stats["signals"],
            "positions": len(a_positions),
            "hit_rates": a_hit_rates,
            "avg_lead_time": round(sum(a_leads) / len(a_leads), 1) if a_leads else None,
        })

    # --- Excursion stats ---
    favorable_excursions = [p["max_favorable_excursion"] for p in scoreable
                            if p.get("max_favorable_excursion") is not None]
    adverse_excursions = [p["max_adverse_excursion"] for p in scoreable
                          if p.get("max_adverse_excursion") is not None]

    # Best performing cascade category
    best_category = leaderboard[0]["category"] if leaderboard and leaderboard[0]["hit_rate_24h"] else "N/A"

    return {
        "summary": {
            "total_signals": total_signals,
            "total_positions": total_positions,
            "active_positions": active_positions,
            "closed_positions": closed_positions,
            "scoreable_positions": len(scoreable),
            "best_category": best_category,
        },
        "hit_rates": hit_rates,
        "avg_lead_time_hours": avg_lead_time,
        "advantage_distribution": advantage_dist,
        "advantage_states": dict(advantage_states),
        "leaderboard": leaderboard,
        "strength_stats": strength_stats,
        "asset_breakdown": asset_breakdown,
        "excursions": {
            "avg_favorable": round(sum(favorable_excursions) / len(favorable_excursions), 2) if favorable_excursions else None,
            "avg_adverse": round(sum(adverse_excursions) / len(adverse_excursions), 2) if adverse_excursions else None,
        },
        "positions": positions,
        "signals": signals,
        "scores": scores,
    }


def generate_claude_briefing():
    """Generate a text briefing suitable for Claude analysis."""
    data = generate_analytics()
    s = data["summary"]
    hr = data["hit_rates"]

    lines = [
        "=== NAS Signal Tester Briefing ===",
        "",
        "Signals received: {}".format(s["total_signals"]),
        "Positions tracked: {} ({} active, {} closed)".format(
            s["total_positions"], s["active_positions"], s["closed_positions"]),
        "Scoreable positions: {}".format(s["scoreable_positions"]),
        "",
        "--- Hit Rates by Horizon ---",
    ]
    for h in HORIZONS:
        r = hr.get(h, {})
        if r.get("rate") is not None:
            lines.append("  {}h: {:.1f}% ({}/{})".format(h, r["rate"], r["hits"], r["total"]))
        else:
            lines.append("  {}h: pending".format(h))

    lines.append("")
    if data["avg_lead_time_hours"] is not None:
        lines.append("Average lead time: {:.1f}h".format(data["avg_lead_time_hours"]))
    else:
        lines.append("Average lead time: pending")

    lines.append("")
    lines.append("--- Cascade Leaderboard (by 24h hit rate) ---")
    for entry in data["leaderboard"][:10]:
        rate_str = "{:.0f}%".format(entry["hit_rate_24h"]) if entry["hit_rate_24h"] is not None else "pending"
        lines.append("  {}: {} ({} positions, tickers: {})".format(
            entry["category"], rate_str, entry["positions"],
            ", ".join(entry["tickers"][:3])))

    lines.append("")
    lines.append("--- Advantage Distribution ---")
    for bucket, count in data["advantage_distribution"].items():
        lines.append("  {}: {}".format(bucket, count))

    lines.append("")
    lines.append("Best category: {}".format(s["best_category"]))

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from db import init_db
    init_db()
    print(generate_claude_briefing())
