"""
NAS Signal Tester - HTML Report Generator
Noah Pink dashboard with Advantage Clock timelines.
Three-act structure: Advantage Board > Cascade Leaderboard > The Proof.
"""
import json
import os
import logging
from datetime import datetime, timezone, timedelta

from db import get_conn
from config import HORIZONS, BASE_DIR, REPORTS_DIR
from analytics import generate_analytics

logger = logging.getLogger("nas.report")

# --- Colour helpers ---

def _direction_colour(direction):
    if direction == "UP":
        return "#0d7680"
    elif direction == "DOWN":
        return "#c0392b"
    return "#666"


def _direction_arrow(direction):
    if direction == "UP":
        return "&#9650;"  # up triangle
    elif direction == "DOWN":
        return "&#9660;"  # down triangle
    return "&#9654;"


def _strength_colour(strength):
    s = (strength or "").lower()
    if s == "high":
        return "#0d7680"
    elif s == "moderate":
        return "#d4a843"
    return "#888"


def _advantage_state_badge(state, lead_hours=None):
    """Return HTML badge for advantage state."""
    state = state or "AHEAD"
    if state == "AHEAD":
        hours_str = ""
        if lead_hours is not None:
            hours_str = " ({:.0f}h)".format(lead_hours)
        return '<span class="adv-badge adv-ahead">&#9719; Ahead of Market{}</span>'.format(hours_str)
    elif state == "CONFIRMED":
        hours_str = ""
        if lead_hours is not None:
            hours_str = " at +{:.1f}h".format(lead_hours)
        return '<span class="adv-badge adv-confirmed">&#10003; Confirmed{}</span>'.format(hours_str)
    elif state == "PEAKED":
        hours_str = ""
        if lead_hours is not None:
            hours_str = " at +{:.1f}h".format(lead_hours)
        return '<span class="adv-badge adv-peaked">&#9733; Peaked{}</span>'.format(hours_str)
    elif state == "EXPIRED":
        return '<span class="adv-badge adv-expired">&#10005; Expired</span>'
    elif state == "DIVERGED":
        return '<span class="adv-badge adv-expired">&#10005; Diverged</span>'
    return '<span class="adv-badge adv-ahead">{}</span>'.format(state)


def _return_cell(return_pct):
    """Format a return percentage with colour."""
    if return_pct is None:
        return '<span class="ret-pending">--</span>'
    if return_pct > 0:
        return '<span class="ret-pos">+{:.2f}%</span>'.format(return_pct)
    elif return_pct < 0:
        return '<span class="ret-neg">{:.2f}%</span>'.format(return_pct)
    return '<span class="ret-flat">0.00%</span>'


def _hit_cell(hit_val):
    """Format a hit/miss with icon."""
    if hit_val is None:
        return '<span class="hit-pending">&#8943;</span>'
    if hit_val == 1:
        return '<span class="hit-yes">&#10003;</span>'
    return '<span class="hit-no">&#10005;</span>'


def _advantage_timeline_svg(t_belief, t_reality, t_peak, hours_elapsed,
                            position_id=None, expected_direction=None,
                            max_hours=96, min_move=0.5):
    """Generate an inline SVG advantage timeline — a continuous colour bar.

    Concept: "bar chart without an axis"
    - Each time slice is coloured by whether the return confirms the signal:
      Grey (#ccc5bb)   = waiting — return hasn't crossed threshold yet
      Green (#27ae60)  = confirmed — moving in expected direction
      Gold (#d4a843)   = small dip in a favourable trend (smoothed noise)
      Red (#c0392b)    = diverging — moving against expected direction
    - Uses a 3-sample rolling average to smooth noise / minor fluctuations
    - Peak marker = gold diamond at max favourable point
    - T0 marker = dark dot at the start

    If no price samples exist yet, falls back to a simple elapsed-time bar.
    """
    width = 280
    height = 36
    bar_y = 10
    bar_h = 12

    def h_to_x(h):
        return max(4, min(width - 4, int(h / max_hours * (width - 8)) + 4))

    svg_parts = [
        '<svg width="{}" height="{}" viewBox="0 0 {} {}"'
        ' xmlns="http://www.w3.org/2000/svg">'.format(width, height, width, height),
        # Full background track (pale, represents the monitoring window)
        '<rect x="4" y="{}" width="{}" height="{}" rx="4" fill="#e8ddd4"/>'.format(
            bar_y, width - 8, bar_h),
    ]

    # --- Try to get actual price samples for this position ---
    samples = []
    if position_id:
        try:
            from db import get_conn as _gc
            conn = _gc()
            rows = conn.execute(
                'SELECT hours_since_open, return_pct FROM price_samples '
                'WHERE position_id = ? ORDER BY timestamp',
                (position_id,)
            ).fetchall()
            conn.close()
            samples = [(r['hours_since_open'] or 0, r['return_pct'] or 0) for r in rows]
        except Exception:
            samples = []

    is_down = (expected_direction or "").upper() == "DOWN"

    if len(samples) >= 2:
        # --- Smooth returns with 3-sample rolling average ---
        returns = [r for _, r in samples]
        smoothed = []
        for i in range(len(returns)):
            window_start = max(0, i - 1)
            window_end = min(len(returns), i + 2)
            avg = sum(returns[window_start:window_end]) / (window_end - window_start)
            smoothed.append(avg)

        # --- Build coloured segments ---
        # For DOWN-expected positions, invert: negative return = favourable
        def is_favourable(ret):
            if is_down:
                return ret < -min_move
            return ret > min_move

        def is_adverse(ret):
            if is_down:
                return ret > min_move
            return ret < -min_move

        # Track whether we've EVER been green (for gold/noise handling)
        ever_green = False
        segments = []  # (start_x, end_x, colour)

        for i, (hours, _) in enumerate(samples):
            s_ret = smoothed[i]
            next_hours = samples[i + 1][0] if i + 1 < len(samples) else (hours_elapsed or hours + 0.5)
            x1 = h_to_x(hours)
            x2 = h_to_x(min(next_hours, max_hours))
            if x2 <= x1:
                x2 = x1 + 1

            if is_favourable(s_ret):
                colour = "#27ae60"  # green — confirmed
                ever_green = True
            elif is_adverse(s_ret):
                if ever_green:
                    colour = "#d4a843"  # gold — dip in a trend that was green
                else:
                    colour = "#c0392b"  # red — moving wrong way from the start
            else:
                if ever_green:
                    colour = "#27ae60"  # was green, minor pullback inside threshold
                else:
                    colour = "#ccc5bb"  # grey — waiting / no signal yet
            segments.append((x1, x2, colour))

        for x1, x2, colour in segments:
            svg_parts.append(
                '<rect x="{}" y="{}" width="{}" height="{}" fill="{}" opacity="0.85"/>'.format(
                    x1, bar_y, max(1, x2 - x1), bar_h, colour))

        # Round the leftmost and rightmost edges
        if segments:
            svg_parts.append(
                '<rect x="4" y="{}" width="4" height="{}" rx="4" ry="4" fill="{}"/>'.format(
                    bar_y, bar_h, segments[0][2]))
            last_x = segments[-1][1]
            svg_parts.append(
                '<rect x="{}" y="{}" width="4" height="{}" rx="4" ry="4" fill="{}"/>'.format(
                    max(4, last_x - 4), bar_y, bar_h, segments[-1][2]))

    else:
        # Fallback: simple elapsed-time bar (grey = waiting)
        if hours_elapsed and hours_elapsed > 0:
            elapsed_w = h_to_x(min(hours_elapsed, max_hours)) - 4
            svg_parts.append(
                '<rect x="4" y="{}" width="{}" height="{}" rx="4" fill="#ccc5bb" opacity="0.85"/>'.format(
                    bar_y, max(2, elapsed_w), bar_h))

    # --- Peak marker (gold diamond) ---
    if t_peak and t_belief:
        try:
            tb = datetime.fromisoformat(t_belief) if isinstance(t_belief, str) else t_belief
            tp = datetime.fromisoformat(t_peak) if isinstance(t_peak, str) else t_peak
            if tb.tzinfo is None:
                tb = tb.replace(tzinfo=timezone.utc)
            if tp.tzinfo is None:
                tp = tp.replace(tzinfo=timezone.utc)
            peak_hours = (tp - tb).total_seconds() / 3600
            peak_x = h_to_x(peak_hours)
            cy = bar_y + bar_h // 2
            svg_parts.append(
                '<polygon points="{},{} {},{} {},{} {},{}" fill="#d4a843" '
                'stroke="#fff" stroke-width="1"/>'.format(
                    peak_x, cy - 6,
                    peak_x + 5, cy,
                    peak_x, cy + 6,
                    peak_x - 5, cy))
        except Exception:
            pass

    # --- T_reality marker (green ring — the moment the market confirmed) ---
    if t_reality and t_belief:
        try:
            tb = datetime.fromisoformat(t_belief) if isinstance(t_belief, str) else t_belief
            tr = datetime.fromisoformat(t_reality) if isinstance(t_reality, str) else t_reality
            if tb.tzinfo is None:
                tb = tb.replace(tzinfo=timezone.utc)
            if tr.tzinfo is None:
                tr = tr.replace(tzinfo=timezone.utc)
            reality_hours = (tr - tb).total_seconds() / 3600
            if reality_hours > 0:
                rx = h_to_x(reality_hours)
                svg_parts.append(
                    '<circle cx="{}" cy="{}" r="5" fill="none" '
                    'stroke="#27ae60" stroke-width="2"/>'.format(rx, bar_y + bar_h // 2))
        except Exception:
            pass

    # --- T0 marker (dark dot at start) ---
    svg_parts.append(
        '<circle cx="4" cy="{}" r="4" fill="#262a33" stroke="#fff" stroke-width="1"/>'.format(
            bar_y + bar_h // 2))

    # --- Labels ---
    svg_parts.append(
        '<text x="3" y="{}" font-size="8" fill="#888" '
        'font-family="Montserrat,sans-serif">T0</text>'.format(height - 1))

    if hours_elapsed:
        elapsed_x = h_to_x(min(hours_elapsed, max_hours))
        svg_parts.append(
            '<text x="{}" y="{}" font-size="8" fill="#888" '
            'font-family="Montserrat,sans-serif" text-anchor="end">'
            '{:.0f}h</text>'.format(
                elapsed_x, height - 1, min(hours_elapsed, max_hours)))

    svg_parts.append('</svg>')
    return '\n'.join(svg_parts)


# --- Section builders ---

def _build_hero(data):
    """Build the dark hero section with headline stats."""
    s = data["summary"]
    hr = data["hit_rates"]

    # Dynamic headline
    parts = []
    if s["total_signals"] > 0:
        parts.append("{} signals tracked".format(s["total_signals"]))
    hr24 = hr.get(24, {})
    if hr24.get("rate") is not None:
        parts.append("{:.0f}% directional accuracy at 24h".format(hr24["rate"]))
    if data.get("avg_lead_time_hours") is not None:
        parts.append("Avg {:.1f}h advantage".format(data["avg_lead_time_hours"]))
    headline = " &#8212; ".join(parts) if parts else "Awaiting first signals"

    # Stat grid
    grid_items = [
        ("Total Signals", str(s["total_signals"])),
        ("Active Positions", str(s["active_positions"])),
    ]
    if hr24.get("rate") is not None:
        grid_items.append(("Hit Rate (24h)", "{:.0f}%".format(hr24["rate"])))
    else:
        grid_items.append(("Hit Rate (24h)", "Pending"))
    if data.get("avg_lead_time_hours") is not None:
        grid_items.append(("Avg Advantage", "{:.1f}h".format(data["avg_lead_time_hours"])))
    else:
        grid_items.append(("Avg Advantage", "Pending"))
    grid_items.append(("Best Category", s.get("best_category", "N/A")))

    grid_html = ""
    for label, value in grid_items:
        grid_html += """
        <div class="stat-item">
            <div class="stat-value">{}</div>
            <div class="stat-label">{}</div>
        </div>""".format(value, label)

    return """
    <section class="hero">
        <div class="hero-inner">
            <h1 class="hero-title">SENTIMENT EDGE LAB</h1>
            <p class="hero-subtitle">{}</p>
            <div class="stat-grid">{}</div>
        </div>
    </section>""".format(headline, grid_html)


def _build_active_table(positions, scores):
    """Build the active positions table with advantage clocks."""
    active = [p for p in positions if p.get("is_active")]
    if not active:
        return """
        <div class="section">
            <div class="section-bar bar-active">
                <h2>Active Positions</h2>
                <span class="section-count">0 positions</span>
            </div>
            <p class="empty-state">No active positions. Waiting for NAS signals...</p>
        </div>"""

    rows = ""
    for p in active:
        direction = p.get("expected_direction", "UP")
        ticker = p.get("proxy_ticker", "???")
        category = p.get("cascade_category", "")

        # Current return from latest score or denormalised
        current_ret = None
        pos_scores = [s for s in scores if s["position_id"] == p["id"]]
        if pos_scores:
            latest = sorted(pos_scores, key=lambda x: x["horizon_hours"], reverse=True)
            current_ret = latest[0].get("return_pct")

        entry = p.get("entry_price")
        entry_str = "${:.2f}".format(entry) if entry else "--"

        # Strength badge
        strength = p.get("signal_strength", "")
        strength_html = '<span class="strength-badge" style="background:{};">{}</span>'.format(
            _strength_colour(strength), strength or "?")

        # Horizon hits
        hit_cells = ""
        for h in HORIZONS:
            key = "hit_{}h".format(h)
            hit_cells += '<td class="hit-cell">{}</td>'.format(_hit_cell(p.get(key)))

        # Advantage state
        adv_state = p.get("advantage_state", "AHEAD")
        lead_h = p.get("lead_time_hours")
        adv_badge = _advantage_state_badge(adv_state, lead_h)

        # Advantage timeline SVG
        hours_elapsed = None
        if p.get("opened_at"):
            try:
                opened = datetime.fromisoformat(p["opened_at"])
                if opened.tzinfo is None:
                    opened = opened.replace(tzinfo=timezone.utc)
                hours_elapsed = (datetime.now(timezone.utc) - opened).total_seconds() / 3600
            except Exception:
                pass

        timeline = _advantage_timeline_svg(
            p.get("t_belief"), p.get("t_reality"), p.get("t_peak"),
            hours_elapsed,
            position_id=p.get("id"),
            expected_direction=p.get("expected_direction"))

        # State indicator
        state = p.get("state", "ACTIVE")
        state_class = "state-active" if state == "ACTIVE" else "state-reinforced"

        # Reinforced indicator
        reinforced = ""
        if p.get("reinforced_count", 0) > 0:
            reinforced = '<span class="reinforce-badge">+{}</span>'.format(p["reinforced_count"])

        rows += """
        <tr class="position-row">
            <td class="cell-ticker">
                <span class="dot {}"></span>
                <strong>{}</strong> {}
                <div class="cell-sub">{}</div>
            </td>
            <td class="cell-direction" style="color:{};">{} {}</td>
            <td class="cell-strength">{}</td>
            <td class="cell-entry">{}</td>
            <td class="cell-return">{}</td>
            {}
            <td class="cell-advantage">{}</td>
            <td class="cell-timeline">{}</td>
        </tr>""".format(
            state_class, ticker, reinforced, category,
            _direction_colour(direction), _direction_arrow(direction), direction,
            strength_html,
            entry_str,
            _return_cell(current_ret),
            hit_cells,
            adv_badge,
            timeline,
        )

    horizon_headers = ""
    for h in HORIZONS:
        horizon_headers += '<th class="th-horizon">{}h</th>'.format(h)

    return """
    <div class="section">
        <div class="section-bar bar-active">
            <h2>Act I: The Advantage Board</h2>
            <span class="section-count">{} active positions</span>
        </div>
        <div class="table-wrap">
        <table class="trading-table">
            <thead>
                <tr>
                    <th>Instrument</th>
                    <th>Direction</th>
                    <th>Strength</th>
                    <th>Entry</th>
                    <th>Return</th>
                    {}
                    <th>Advantage</th>
                    <th>Timeline</th>
                </tr>
            </thead>
            <tbody>{}</tbody>
        </table>
        </div>
    </div>""".format(len(active), horizon_headers, rows)


def _build_closed_table(positions, scores):
    """Build the closed/expired positions table."""
    closed = [p for p in positions if not p.get("is_active")]
    if not closed:
        return ""

    rows = ""
    for p in closed[:30]:  # Limit display
        direction = p.get("expected_direction", "UP")
        ticker = p.get("proxy_ticker", "???")
        category = p.get("cascade_category", "")
        state = p.get("state", "CLOSED")

        # Find best return from scores
        pos_scores = [s for s in scores if s["position_id"] == p["id"]]
        best_ret = None
        for s in pos_scores:
            if s.get("return_pct") is not None:
                if best_ret is None or abs(s["return_pct"]) > abs(best_ret):
                    best_ret = s["return_pct"]

        hit_cells = ""
        for h in HORIZONS:
            key = "hit_{}h".format(h)
            hit_cells += '<td class="hit-cell">{}</td>'.format(_hit_cell(p.get(key)))

        adv_state = p.get("advantage_state", "EXPIRED")
        lead_h = p.get("lead_time_hours")
        adv_badge = _advantage_state_badge(adv_state, lead_h)

        close_reason = p.get("close_reason", "")
        close_short = close_reason[:20] if close_reason else state

        rows += """
        <tr class="closed-row">
            <td class="cell-ticker">
                <span class="dot state-closed"></span>
                <strong>{}</strong>
                <div class="cell-sub">{}</div>
            </td>
            <td style="color:{};">{} {}</td>
            <td>{}</td>
            {}
            <td>{}</td>
            <td class="cell-sub">{}</td>
        </tr>""".format(
            ticker, category,
            _direction_colour(direction), _direction_arrow(direction), direction,
            _return_cell(best_ret),
            hit_cells,
            adv_badge,
            close_short,
        )

    horizon_headers = ""
    for h in HORIZONS:
        horizon_headers += '<th class="th-horizon">{}h</th>'.format(h)

    return """
    <div class="section section-closed">
        <div class="section-bar bar-closed">
            <h2>Closed Positions</h2>
            <span class="section-count">{} positions</span>
        </div>
        <div class="table-wrap">
        <table class="trading-table">
            <thead>
                <tr>
                    <th>Instrument</th>
                    <th>Direction</th>
                    <th>Return</th>
                    {}
                    <th>Advantage</th>
                    <th>Reason</th>
                </tr>
            </thead>
            <tbody>{}</tbody>
        </table>
        </div>
    </div>""".format(len(closed), horizon_headers, rows)


def _build_cascade_leaderboard(leaderboard):
    """Build cascade category cards."""
    if not leaderboard:
        return """
        <div class="section">
            <div class="section-bar bar-cascade">
                <h2>Act II: Cascade Leaderboard</h2>
            </div>
            <p class="empty-state">No cascade data yet.</p>
        </div>"""

    cards = ""
    for i, entry in enumerate(leaderboard):
        rate = entry.get("hit_rate_24h")
        rate_str = "{:.0f}%".format(rate) if rate is not None else "Pending"
        rate_class = ""
        if rate is not None:
            if rate >= 60:
                rate_class = "rate-good"
            elif rate >= 40:
                rate_class = "rate-ok"
            else:
                rate_class = "rate-poor"

        avg_right = entry.get("avg_return_when_right")
        avg_wrong = entry.get("avg_return_when_wrong")
        avg_lead = entry.get("avg_lead_time_hours")

        tickers = ", ".join(entry.get("tickers", [])[:4])
        border_class = "card-top" if i == 0 and rate and rate >= 50 else ""

        cards += """
        <div class="cascade-card {}">
            <div class="cascade-header">
                <h3>{}</h3>
                <span class="cascade-rate {}">{}</span>
            </div>
            <div class="cascade-stats">
                <div class="cs-item">
                    <span class="cs-val">{}</span>
                    <span class="cs-label">Positions</span>
                </div>
                <div class="cs-item">
                    <span class="cs-val">{}</span>
                    <span class="cs-label">Avg Lead</span>
                </div>
                <div class="cs-item">
                    <span class="cs-val">{}</span>
                    <span class="cs-label">Avg Right</span>
                </div>
                <div class="cs-item">
                    <span class="cs-val">{}</span>
                    <span class="cs-label">Avg Wrong</span>
                </div>
            </div>
            <div class="cascade-tickers">{}</div>
        </div>""".format(
            border_class,
            entry["category"],
            rate_class, rate_str,
            entry["positions"],
            "{:.1f}h".format(avg_lead) if avg_lead is not None else "--",
            "+{:.2f}%".format(avg_right) if avg_right is not None else "--",
            "-{:.2f}%".format(avg_wrong) if avg_wrong is not None else "--",
            tickers or "No tickers",
        )

    return """
    <div class="section">
        <div class="section-bar bar-cascade">
            <h2>Act II: Cascade Leaderboard</h2>
            <span class="section-count">{} categories</span>
        </div>
        <div class="card-grid">{}</div>
    </div>""".format(len(leaderboard), cards)


def _build_proof_section(data):
    """Build Act III: The Proof — horizon analysis, strength, advantage distribution."""
    hr = data["hit_rates"]
    strength = data["strength_stats"]
    adv_dist = data["advantage_distribution"]
    excursions = data["excursions"]
    asset_breakdown = data["asset_breakdown"]

    # Horizon sweet spot
    horizon_rows = ""
    best_h = None
    best_rate = 0
    for h in HORIZONS:
        r = hr.get(h, {})
        rate = r.get("rate")
        if rate is not None and rate > best_rate:
            best_rate = rate
            best_h = h

    for h in HORIZONS:
        r = hr.get(h, {})
        rate = r.get("rate")
        is_best = h == best_h
        bar_w = int(rate) if rate is not None else 0
        bar_class = "bar-best" if is_best else ""

        horizon_rows += """
        <div class="horizon-row">
            <span class="horizon-label">{}h</span>
            <div class="horizon-bar-wrap">
                <div class="horizon-bar {}" style="width:{}%;"></div>
            </div>
            <span class="horizon-val">{}</span>
        </div>""".format(
            h, bar_class, min(bar_w, 100),
            "{:.0f}% ({}/{})".format(rate, r.get("hits", 0), r.get("total", 0)) if rate is not None else "Pending")

    # Signal strength vs accuracy table
    strength_rows = ""
    for s_name in ["High", "Moderate", "Low"]:
        s_data = strength.get(s_name, {})
        count = s_data.get("count", 0)
        s_hr = s_data.get("hit_rates", {})
        cells = ""
        for h in HORIZONS:
            val = s_hr.get(h)
            cells += '<td>{}</td>'.format("{:.0f}%".format(val) if val is not None else "--")
        strength_rows += """
        <tr>
            <td><span class="strength-badge" style="background:{};">{}</span></td>
            <td>{}</td>
            {}
        </tr>""".format(_strength_colour(s_name), s_name, count, cells)

    strength_headers = ""
    for h in HORIZONS:
        strength_headers += '<th>{}h</th>'.format(h)

    # Advantage distribution
    adv_bars = ""
    max_adv = max(adv_dist.values()) if adv_dist and max(adv_dist.values()) > 0 else 1
    for bucket, count in adv_dist.items():
        bar_w = int(count / max_adv * 100)
        adv_bars += """
        <div class="adv-dist-row">
            <span class="adv-dist-label">{}</span>
            <div class="adv-dist-bar-wrap">
                <div class="adv-dist-bar" style="width:{}%;"></div>
            </div>
            <span class="adv-dist-val">{}</span>
        </div>""".format(bucket, bar_w, count)

    # Excursion summary
    exc_html = ""
    if excursions.get("avg_favorable") is not None:
        exc_html = """
        <div class="proof-card">
            <h3>Excursion Analysis</h3>
            <div class="exc-grid">
                <div class="exc-item exc-fav">
                    <div class="exc-val">+{:.2f}%</div>
                    <div class="exc-label">Avg Max Favorable</div>
                </div>
                <div class="exc-item exc-adv">
                    <div class="exc-val">{:.2f}%</div>
                    <div class="exc-label">Avg Max Adverse</div>
                </div>
            </div>
        </div>""".format(
            excursions["avg_favorable"],
            excursions.get("avg_adverse", 0))

    # Per-asset breakdown
    asset_rows = ""
    for a in asset_breakdown:
        a_hr = a.get("hit_rates", {})
        cells = ""
        for h in HORIZONS:
            val = a_hr.get(h)
            cells += '<td>{}</td>'.format("{:.0f}%".format(val) if val is not None else "--")
        avg_lead = a.get("avg_lead_time")
        asset_rows += """
        <tr>
            <td><strong>{}</strong></td>
            <td>{}</td>
            <td>{}</td>
            {}
            <td>{}</td>
        </tr>""".format(
            a["asset"],
            a.get("signals", 0),
            a.get("positions", 0),
            cells,
            "{:.1f}h".format(avg_lead) if avg_lead is not None else "--")

    asset_headers = ""
    for h in HORIZONS:
        asset_headers += '<th>{}h</th>'.format(h)

    return """
    <div class="section section-proof">
        <div class="section-bar bar-proof">
            <h2>Act III: The Proof</h2>
        </div>

        <div class="proof-grid">
            <div class="proof-card">
                <h3>Horizon Sweet Spot</h3>
                <p class="proof-desc">Which horizon delivers the best directional accuracy?</p>
                {}
            </div>

            <div class="proof-card">
                <h3>Advantage Distribution</h3>
                <p class="proof-desc">How far ahead of the market are our signals?</p>
                {}
            </div>
        </div>

        <div class="proof-card proof-wide">
            <h3>Signal Strength vs Accuracy</h3>
            <table class="proof-table">
                <thead>
                    <tr>
                        <th>Strength</th>
                        <th>Count</th>
                        {}
                    </tr>
                </thead>
                <tbody>{}</tbody>
            </table>
        </div>

        {}

        <div class="proof-card proof-wide">
            <h3>Per-Asset Breakdown</h3>
            <table class="proof-table">
                <thead>
                    <tr>
                        <th>Asset</th>
                        <th>Signals</th>
                        <th>Positions</th>
                        {}
                        <th>Avg Lead</th>
                    </tr>
                </thead>
                <tbody>{}</tbody>
            </table>
        </div>
    </div>""".format(
        horizon_rows,
        adv_bars,
        strength_headers, strength_rows,
        exc_html,
        asset_headers, asset_rows)


def _build_signals_table(signals):
    """Build recent signals table."""
    if not signals:
        return ""

    rows = ""
    for s in signals[:20]:
        direction = s.get("direction_bias", "")
        strength = s.get("signal_strength", "")
        pressure = s.get("pressure_index")

        rows += """
        <tr>
            <td><strong>{}</strong></td>
            <td>{}</td>
            <td style="color:{};">{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>""".format(
            s.get("signal_id", "")[:12],
            s.get("primary_asset", "Unknown"),
            _direction_colour("UP" if direction and "Bull" in direction else "DOWN" if direction and "Bear" in direction else ""),
            direction or "--",
            '<span class="strength-badge" style="background:{};">{}</span>'.format(
                _strength_colour(strength), strength) if strength else "--",
            "{:.1f}".format(pressure) if pressure is not None else "--",
            s.get("ingested_at", "")[:16],
        )

    return """
    <div class="section">
        <div class="section-bar bar-signals">
            <h2>Recent Signals</h2>
            <span class="section-count">{} total</span>
        </div>
        <div class="table-wrap">
        <table class="trading-table signals-table">
            <thead>
                <tr>
                    <th>Signal ID</th>
                    <th>Asset</th>
                    <th>Direction</th>
                    <th>Strength</th>
                    <th>Pressure</th>
                    <th>Ingested</th>
                </tr>
            </thead>
            <tbody>{}</tbody>
        </table>
        </div>
    </div>""".format(len(signals), rows)


# --- Main report generator ---

def generate_report():
    """Generate the full HTML dashboard. Returns the HTML string."""
    data = generate_analytics()
    now = datetime.now(timezone.utc)

    hero = _build_hero(data)
    active_table = _build_active_table(data["positions"], data["scores"])
    closed_table = _build_closed_table(data["positions"], data["scores"])
    cascade_lb = _build_cascade_leaderboard(data["leaderboard"])
    proof = _build_proof_section(data)
    signals_table = _build_signals_table(data["signals"])

    # OG description
    s = data["summary"]
    hr24 = data["hit_rates"].get(24, {})
    og_parts = []
    if s["active_positions"] > 0:
        og_parts.append("{} active positions".format(s["active_positions"]))
    if hr24.get("rate") is not None:
        og_parts.append("{:.0f}% hit rate at 24h".format(hr24["rate"]))
    if data.get("avg_lead_time_hours") is not None:
        og_parts.append("{:.1f}h avg advantage".format(data["avg_lead_time_hours"]))
    og_desc = "Sentiment Edge Lab: " + (", ".join(og_parts) if og_parts else "Awaiting first signals")

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NOAH Sentiment Edge Lab</title>
<meta property="og:type" content="website">
<meta property="og:title" content="NOAH Sentiment Edge Lab">
<meta property="og:description" content="{og_desc}">
<meta property="og:image" content="https://ivanmassow.github.io/nas-tester/og-image.png">
<meta property="og:url" content="https://ivanmassow.github.io/nas-tester/">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="NOAH Sentiment Edge Lab">
<meta name="twitter:description" content="{og_desc}">
<meta name="twitter:image" content="https://ivanmassow.github.io/nas-tester/og-image.png">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700&family=Lato:wght@300;400;700&family=Montserrat:wght@500;600;700&display=swap" rel="stylesheet">
<style>
/* === Noah Pink Design System === */
:root {{
    --ink: #262a33;
    --paper: #FFF1E5;
    --blush: #ffe4d6;
    --accent: #0d7680;
    --accent-light: #e0f2f1;
    --gold: #d4a843;
    --green: #27ae60;
    --red: #c0392b;
    --grey: #888;
    --light-grey: #f5f0eb;
    --border: #e8ddd4;
}}

* {{ margin: 0; padding: 0; box-sizing: border-box; }}

body {{
    font-family: 'Lato', -apple-system, sans-serif;
    background: var(--paper);
    color: var(--ink);
    line-height: 1.6;
    min-height: 100vh;
}}

/* === Header === */
.header {{
    background: var(--ink);
    padding: 10px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
}}
.header-logo {{
    color: #fff;
    font-family: 'Montserrat', sans-serif;
    font-weight: 700;
    font-size: 1.3rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}}
.header-nav {{
    display: flex;
    gap: 16px;
}}
.header-nav a {{
    color: rgba(255,255,255,0.6);
    text-decoration: none;
    font-size: 12px;
    font-family: 'Montserrat', sans-serif;
    transition: color 0.2s;
}}
.header-nav a:hover {{ color: #fff; }}
.header-meta {{
    color: rgba(255,255,255,0.4);
    font-size: 11px;
    font-family: 'Montserrat', sans-serif;
}}

/* === Hero === */
.hero {{
    background: var(--ink);
    padding: 40px 24px 48px;
    text-align: center;
}}
.hero-inner {{
    max-width: 960px;
    margin: 0 auto;
}}
.hero-title {{
    font-family: 'Playfair Display', serif;
    font-size: 36px;
    color: #fff;
    letter-spacing: 4px;
    margin-bottom: 10px;
}}
.hero-subtitle {{
    color: rgba(255,255,255,0.7);
    font-size: 15px;
    font-family: 'Lato', sans-serif;
    margin-bottom: 28px;
}}
.stat-grid {{
    display: flex;
    justify-content: center;
    gap: 32px;
    flex-wrap: wrap;
}}
.stat-item {{
    text-align: center;
    min-width: 100px;
}}
.stat-value {{
    font-family: 'Montserrat', sans-serif;
    font-weight: 700;
    font-size: 28px;
    color: var(--gold);
}}
.stat-label {{
    font-family: 'Montserrat', sans-serif;
    font-size: 10px;
    color: rgba(255,255,255,0.6);
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-top: 4px;
}}

/* === Sections === */
.section {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 24px;
    margin-bottom: 40px;
}}
.section-bar {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-left: 4px solid var(--accent);
    padding: 12px 16px;
    margin: 32px 0 16px;
    background: rgba(13,118,128,0.04);
    border-radius: 0 6px 6px 0;
}}
.section-bar h2 {{
    font-family: 'Playfair Display', serif;
    font-size: 20px;
    color: var(--ink);
}}
.section-count {{
    font-family: 'Montserrat', sans-serif;
    font-size: 12px;
    color: var(--grey);
    text-transform: uppercase;
    letter-spacing: 1px;
}}
.bar-active {{ border-color: var(--accent); }}
.bar-closed {{ border-color: var(--grey); }}
.bar-cascade {{ border-color: var(--gold); }}
.bar-proof {{ border-color: var(--green); }}
.bar-signals {{ border-color: #6c5ce7; }}

.empty-state {{
    text-align: center;
    padding: 40px;
    color: var(--grey);
    font-style: italic;
}}

/* === Tables === */
.table-wrap {{ overflow-x: auto; }}
.trading-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}}
.trading-table th {{
    font-family: 'Montserrat', sans-serif;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--grey);
    padding: 8px 10px;
    border-bottom: 2px solid var(--border);
    text-align: left;
    white-space: nowrap;
}}
.trading-table td {{
    padding: 10px;
    border-bottom: 1px solid var(--border);
    vertical-align: middle;
}}
.trading-table tbody tr:hover {{
    background: rgba(13,118,128,0.03);
}}
.th-horizon {{
    text-align: center;
    min-width: 40px;
}}
.hit-cell {{ text-align: center; }}

.cell-ticker {{ min-width: 120px; }}
.cell-sub {{
    font-size: 11px;
    color: var(--grey);
    margin-top: 2px;
}}
.cell-direction {{
    font-weight: 700;
    white-space: nowrap;
}}
.cell-timeline {{ min-width: 280px; }}
.cell-advantage {{ white-space: nowrap; }}

/* === Status dots === */
.dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 6px;
    vertical-align: middle;
}}
.state-active {{ background: var(--accent); }}
.state-reinforced {{ background: var(--green); }}
.state-closed {{ background: var(--grey); }}

/* === Badges === */
.strength-badge {{
    font-family: 'Montserrat', sans-serif;
    font-size: 10px;
    padding: 2px 8px;
    border-radius: 10px;
    color: #fff;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.reinforce-badge {{
    font-family: 'Montserrat', sans-serif;
    font-size: 9px;
    background: var(--green);
    color: #fff;
    padding: 1px 5px;
    border-radius: 8px;
    margin-left: 4px;
    vertical-align: middle;
}}

/* === Advantage badges === */
.adv-badge {{
    font-family: 'Montserrat', sans-serif;
    font-size: 11px;
    padding: 3px 10px;
    border-radius: 12px;
    white-space: nowrap;
}}
.adv-ahead {{
    background: rgba(13,118,128,0.12);
    color: var(--accent);
    border: 1px solid rgba(13,118,128,0.3);
}}
.adv-confirmed {{
    background: rgba(39,174,96,0.12);
    color: var(--green);
    border: 1px solid rgba(39,174,96,0.3);
}}
.adv-peaked {{
    background: rgba(212,168,67,0.12);
    color: #b8860b;
    border: 1px solid rgba(212,168,67,0.3);
}}
.adv-expired {{
    background: rgba(136,136,136,0.1);
    color: var(--grey);
    border: 1px solid rgba(136,136,136,0.2);
}}

/* === Return colours === */
.ret-pos {{ color: var(--green); font-weight: 700; }}
.ret-neg {{ color: var(--red); font-weight: 700; }}
.ret-flat {{ color: var(--grey); }}
.ret-pending {{ color: #ccc; }}

/* === Hit indicators === */
.hit-yes {{ color: var(--green); font-size: 16px; }}
.hit-no {{ color: var(--red); font-size: 14px; }}
.hit-pending {{ color: #ccc; font-size: 14px; }}

/* === Cascade Cards === */
.card-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
    gap: 16px;
    margin-top: 16px;
}}
.cascade-card {{
    background: #fff;
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
    transition: box-shadow 0.2s;
}}
.cascade-card:hover {{
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
}}
.card-top {{
    border-left: 4px solid var(--accent);
}}
.cascade-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
}}
.cascade-header h3 {{
    font-family: 'Playfair Display', serif;
    font-size: 15px;
    text-transform: capitalize;
}}
.cascade-rate {{
    font-family: 'Montserrat', sans-serif;
    font-weight: 700;
    font-size: 18px;
}}
.rate-good {{ color: var(--green); }}
.rate-ok {{ color: var(--gold); }}
.rate-poor {{ color: var(--red); }}
.cascade-stats {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
    margin-bottom: 10px;
}}
.cs-item {{ text-align: center; }}
.cs-val {{
    font-family: 'Montserrat', sans-serif;
    font-weight: 600;
    font-size: 13px;
    display: block;
}}
.cs-label {{
    font-size: 9px;
    color: var(--grey);
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.cascade-tickers {{
    font-size: 11px;
    color: var(--grey);
    font-family: 'Montserrat', sans-serif;
}}

/* === Proof Section === */
.section-proof {{ margin-top: 20px; }}
.proof-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-top: 16px;
}}
.proof-card {{
    background: #fff;
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
}}
.proof-wide {{ margin-top: 16px; }}
.proof-card h3 {{
    font-family: 'Playfair Display', serif;
    font-size: 16px;
    margin-bottom: 6px;
}}
.proof-desc {{
    font-size: 12px;
    color: var(--grey);
    margin-bottom: 14px;
}}
.proof-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}}
.proof-table th {{
    font-family: 'Montserrat', sans-serif;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--grey);
    padding: 6px 8px;
    border-bottom: 2px solid var(--border);
    text-align: left;
}}
.proof-table td {{
    padding: 8px;
    border-bottom: 1px solid var(--border);
}}

/* === Horizon bars === */
.horizon-row {{
    display: flex;
    align-items: center;
    margin-bottom: 8px;
}}
.horizon-label {{
    font-family: 'Montserrat', sans-serif;
    font-weight: 600;
    font-size: 13px;
    width: 36px;
}}
.horizon-bar-wrap {{
    flex: 1;
    height: 20px;
    background: var(--light-grey);
    border-radius: 4px;
    margin: 0 10px;
    overflow: hidden;
}}
.horizon-bar {{
    height: 100%;
    background: var(--accent);
    border-radius: 4px;
    transition: width 0.6s ease;
    opacity: 0.75;
}}
.bar-best {{
    background: var(--accent);
    opacity: 1;
}}
.horizon-val {{
    font-family: 'Montserrat', sans-serif;
    font-size: 12px;
    min-width: 90px;
    text-align: right;
    color: var(--grey);
}}

/* === Advantage distribution bars === */
.adv-dist-row {{
    display: flex;
    align-items: center;
    margin-bottom: 6px;
}}
.adv-dist-label {{
    font-family: 'Montserrat', sans-serif;
    font-size: 12px;
    width: 50px;
    color: var(--grey);
}}
.adv-dist-bar-wrap {{
    flex: 1;
    height: 16px;
    background: var(--light-grey);
    border-radius: 3px;
    margin: 0 8px;
    overflow: hidden;
}}
.adv-dist-bar {{
    height: 100%;
    background: var(--accent);
    border-radius: 3px;
    opacity: 0.8;
}}
.adv-dist-val {{
    font-family: 'Montserrat', sans-serif;
    font-size: 12px;
    min-width: 30px;
    text-align: right;
    font-weight: 600;
}}

/* === Excursion === */
.exc-grid {{
    display: flex;
    gap: 24px;
    margin-top: 12px;
}}
.exc-item {{
    text-align: center;
    flex: 1;
    padding: 12px;
    border-radius: 8px;
}}
.exc-fav {{ background: rgba(39,174,96,0.08); }}
.exc-adv {{ background: rgba(192,57,43,0.06); }}
.exc-val {{
    font-family: 'Montserrat', sans-serif;
    font-weight: 700;
    font-size: 22px;
}}
.exc-fav .exc-val {{ color: var(--green); }}
.exc-adv .exc-val {{ color: var(--red); }}
.exc-label {{
    font-size: 11px;
    color: var(--grey);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-top: 4px;
}}

/* === Footer === */
.footer {{
    background: var(--ink);
    padding: 32px 24px;
    margin-top: 60px;
    text-align: center;
}}
.footer-links {{
    margin-bottom: 16px;
}}
.footer-links a {{
    color: rgba(255,255,255,0.5);
    text-decoration: none;
    margin: 0 10px;
    font-size: 12px;
    font-family: 'Montserrat', sans-serif;
}}
.footer-links a:hover {{ color: #fff; }}
.footer-disclaimer {{
    color: rgba(255,255,255,0.3);
    font-size: 10px;
    max-width: 640px;
    margin: 0 auto;
    line-height: 1.8;
}}

/* === Responsive === */
@media (max-width: 768px) {{
    .hero-title {{ font-size: 24px; letter-spacing: 2px; }}
    .stat-grid {{ gap: 16px; }}
    .stat-value {{ font-size: 20px; }}
    .proof-grid {{ grid-template-columns: 1fr; }}
    .card-grid {{ grid-template-columns: 1fr; }}
    .cell-timeline {{ min-width: 200px; }}
    .cascade-stats {{ grid-template-columns: repeat(2, 1fr); }}
}}

.closed-row {{ opacity: 0.7; }}
.closed-row:hover {{ opacity: 1; }}
</style>
</head>
<body>

<header class="header">
    <div class="header-logo">NOAH</div>
    <nav class="header-nav">
        <a href="#act-1">Advantage Board</a>
        <a href="#act-2">Cascades</a>
        <a href="#act-3">Proof</a>
    </nav>
    <div class="header-meta">Updated {updated}</div>
</header>

{hero}

<main>
{active_table}
{closed_table}
{cascade_lb}
{proof}
{signals_table}
</main>

<footer class="footer">
    <div class="footer-links">
        <a href="https://ivanmassow.github.io/hedgefund-tracker/">Hedge Fund Tracker</a>
        <a href="https://ivanmassow.github.io/nas-tester/">Sentiment Edge Lab</a>
    </div>
    <p class="footer-disclaimer">
        Sentiment Edge Lab is a research tool for benchmarking narrative-driven asset signals.
        No real trades are executed. Paper positions only. Not financial advice.<br>
        Generated {generated}
    </p>
</footer>

</body>
</html>""".format(
        og_desc=og_desc,
        updated=now.strftime("%Y-%m-%d %H:%M UTC"),
        hero=hero,
        active_table=active_table,
        closed_table=closed_table,
        cascade_lb=cascade_lb,
        proof=proof,
        signals_table=signals_table,
        generated=now.strftime("%Y-%m-%d %H:%M:%S UTC"),
    )

    return html


def write_report():
    """Generate report and write to disk. Returns the file path."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    html = generate_report()

    report_path = os.path.join(REPORTS_DIR, "latest.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Also copy to index.html for GitHub Pages
    index_path = os.path.join(BASE_DIR, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info("Report written to {} ({:.0f} KB)".format(
        report_path, len(html) / 1024))
    return report_path


def write_summary_json():
    """Export summary.json for external consumption."""
    data = generate_analytics()
    summary = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "summary": data["summary"],
        "hit_rates": data["hit_rates"],
        "avg_lead_time_hours": data["avg_lead_time_hours"],
        "advantage_distribution": data["advantage_distribution"],
        "leaderboard": data["leaderboard"],
    }

    path = os.path.join(BASE_DIR, "summary.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info("Summary JSON written to {}".format(path))
    return path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from db import init_db
    init_db()
    path = write_report()
    print("Report: {}".format(path))
    write_summary_json()
    print("Summary JSON written")
