"""
NAS Signal Tester - Price Tracker
Fetches prices via yfinance (primary) and Alpha Vantage (fallback).
Stores 30-min snapshots for all active positions.
"""
import time
import logging
from datetime import datetime, timezone, timedelta

from db import get_conn
from config import (
    ALPHA_VANTAGE_KEY, ALPHA_VANTAGE_BASE, AV_RATE_LIMIT,
    SAMPLE_INTERVAL_MINUTES, HORIZONS
)
from position_manager import close_expired_positions

logger = logging.getLogger("nas.tracker")


def fetch_price_yf(ticker):
    """Fetch current price from yfinance. No API key needed."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        hist = t.history(period="1d")
        if hist.empty:
            # Try 5d for recently listed or illiquid tickers
            hist = t.history(period="5d")
        if hist.empty:
            logger.warning("yfinance: no data for {}".format(ticker))
            return None
        latest = hist.iloc[-1]
        return {
            "price": float(latest["Close"]),
            "open": float(latest["Open"]),
            "high": float(latest["High"]),
            "low": float(latest["Low"]),
            "volume": float(latest["Volume"]),
            "change_pct": 0.0,  # yfinance doesn't give daily change directly
        }
    except Exception as e:
        logger.warning("yfinance fetch failed for {}: {}".format(ticker, e))
        return None


def fetch_price_av(ticker):
    """Fetch current price from Alpha Vantage Global Quote endpoint (fallback)."""
    if not ALPHA_VANTAGE_KEY:
        return None

    import requests
    try:
        resp = requests.get(
            ALPHA_VANTAGE_BASE,
            params={
                "function": "GLOBAL_QUOTE",
                "symbol": ticker,
                "apikey": ALPHA_VANTAGE_KEY,
            },
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        gq = data.get("Global Quote", {})
        if not gq or "05. price" not in gq:
            if "Note" in data or "Information" in data:
                logger.warning("AV rate limit for {}: {}".format(
                    ticker, data.get("Note", data.get("Information", ""))[:100]))
            return None

        return {
            "price": float(gq["05. price"]),
            "open": float(gq.get("02. open", 0)),
            "high": float(gq.get("03. high", 0)),
            "low": float(gq.get("04. low", 0)),
            "volume": float(gq.get("06. volume", 0)),
            "change_pct": float(gq.get("10. change percent", "0").rstrip('%')),
        }
    except Exception as e:
        logger.warning("AV fetch failed for {}: {}".format(ticker, e))
        return None


def fetch_price(ticker):
    """Fetch price, trying yfinance first, then AV fallback.
    Returns (price_data_dict, source_string) or (None, None).
    """
    data = fetch_price_yf(ticker)
    if data:
        return data, "yfinance"

    logger.info("yfinance failed for {}, trying Alpha Vantage...".format(ticker))
    data = fetch_price_av(ticker)
    if data:
        time.sleep(AV_RATE_LIMIT)
        return data, "alpha_vantage"

    return None, None


def should_snapshot(position_id):
    """Check if we should take a new snapshot (avoid duplicates within sample interval)."""
    conn = get_conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=SAMPLE_INTERVAL_MINUTES - 5)).isoformat()
    existing = conn.execute("""
        SELECT 1 FROM price_samples
        WHERE position_id = ? AND timestamp > ?
    """, (position_id, cutoff)).fetchone()
    conn.close()
    return existing is None


def is_horizon_time(opened_at_str, now, horizons=None):
    """Check if current time is near any configured horizon from position open.
    Returns the horizon value if within 30 min of a horizon, else None.
    """
    if not opened_at_str:
        return None
    if horizons is None:
        horizons = HORIZONS

    opened = datetime.fromisoformat(opened_at_str)
    if opened.tzinfo is None:
        opened = opened.replace(tzinfo=timezone.utc)

    hours_elapsed = (now - opened).total_seconds() / 3600

    for h in horizons:
        # Within 30 minutes of the horizon
        if abs(hours_elapsed - h) < 0.5:
            return h
    return None


def track_prices():
    """Main tracking function. Fetches prices for all active positions."""
    close_expired_positions()

    conn = get_conn()
    positions = conn.execute("""
        SELECT * FROM positions WHERE is_active = 1
    """).fetchall()
    conn.close()

    if not positions:
        logger.info("No active positions to track")
        return 0

    logger.info("Tracking {} active positions".format(len(positions)))
    now = datetime.now(timezone.utc)

    # Group by ticker to avoid duplicate fetches
    ticker_prices = {}
    tickers_needed = set()
    for p in positions:
        if p["proxy_ticker"]:
            tickers_needed.add(p["proxy_ticker"])

    # Fetch prices for unique tickers
    for ticker in sorted(tickers_needed):
        if ticker in ticker_prices:
            continue
        data, source = fetch_price(ticker)
        if data:
            ticker_prices[ticker] = (data, source)

    # Store snapshots
    conn = get_conn()
    tracked = 0
    for p in positions:
        pid = p["id"]
        if not should_snapshot(pid):
            continue

        ticker = p["proxy_ticker"]
        if ticker not in ticker_prices:
            continue

        price_data, source = ticker_prices[ticker]

        # Calculate hours since open and return
        opened = datetime.fromisoformat(p["opened_at"])
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
        hours_since_open = (now - opened).total_seconds() / 3600

        # Calculate return percentage
        return_pct = None
        if p["entry_price"] and p["entry_price"] > 0:
            return_pct = (price_data["price"] - p["entry_price"]) / p["entry_price"] * 100
            return_pct = round(return_pct, 4)

        # Check if this is a horizon sample
        horizon = is_horizon_time(p["opened_at"], now)

        conn.execute("""
            INSERT INTO price_samples
            (position_id, ticker, timestamp, price, open_price, high, low,
             volume, change_pct, hours_since_open, return_pct,
             is_horizon_sample, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pid, ticker, now.isoformat(),
            price_data["price"], price_data["open"],
            price_data["high"], price_data["low"],
            price_data["volume"], price_data["change_pct"],
            round(hours_since_open, 2), return_pct,
            1 if horizon else 0, source
        ))

        # Set entry price on first successful price fetch if not yet set
        if not p["entry_price"]:
            conn.execute("""
                UPDATE positions SET
                    entry_price = ?,
                    entry_price_source = ?,
                    entry_price_time = ?
                WHERE id = ?
            """, (price_data["price"], source, now.isoformat(), pid))
            logger.info("  Entry price set for {} ({}): ${:.2f}".format(
                ticker, p["cascade_category"], price_data["price"]))

        tracked += 1
        logger.debug("  {} ({}): ${:.2f} ret={}%".format(
            ticker, p["cascade_category"][:20], price_data["price"],
            "{:.2f}".format(return_pct) if return_pct is not None else "N/A"))

    conn.commit()
    conn.close()
    logger.info("Tracked {}/{} positions ({} unique tickers)".format(
        tracked, len(positions), len(ticker_prices)))
    return tracked


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        ticker = sys.argv[2] if len(sys.argv) > 2 else "SPY"
        print("Testing price fetch for {}...".format(ticker))
        data, source = fetch_price(ticker)
        if data:
            print("  Source: {}".format(source))
            print("  Price: ${:.2f}".format(data["price"]))
            print("  Volume: {:.0f}".format(data["volume"]))
        else:
            print("  Failed to fetch price")
    else:
        from db import init_db
        init_db()
        track_prices()
