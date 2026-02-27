"""
NAS Signal Tester - Smart Instrument Resolver
Maps exposure_category strings to liquid ETF proxy tickers.
Three-tier lookup: static map -> DB cache -> dynamic search (yfinance + AV).
"""
import logging
import time
from datetime import datetime, timezone

from db import get_conn
from config import ETF_PROXY_MAP, ALPHA_VANTAGE_KEY, ALPHA_VANTAGE_BASE, AV_RATE_LIMIT

logger = logging.getLogger("nas.resolver")


def resolve_proxy(exposure_category):
    """Resolve an exposure_category to a liquid ETF proxy ticker.
    Returns (ticker, method) or (None, 'failed').
    """
    if not exposure_category:
        return None, "failed"

    normalised = exposure_category.lower().strip()

    # Tier 1: Exact match in static map
    if normalised in ETF_PROXY_MAP:
        ticker = ETF_PROXY_MAP[normalised]
        logger.debug("Resolver: '{}' -> {} (static_map)".format(exposure_category, ticker))
        return ticker, "static_map"

    # Tier 1b: Fuzzy match — check if any map key is a substring or vice versa
    for key, ticker in ETF_PROXY_MAP.items():
        if key in normalised or normalised in key:
            logger.debug("Resolver: '{}' -> {} (static_map_fuzzy via '{}')".format(
                exposure_category, ticker, key))
            return ticker, "static_map_fuzzy"

    # Tier 1c: Word overlap — check for significant word matches
    norm_words = set(normalised.split())
    best_overlap = 0
    best_ticker = None
    for key, ticker in ETF_PROXY_MAP.items():
        key_words = set(key.split())
        overlap = len(norm_words & key_words)
        if overlap > best_overlap and overlap >= 1:
            best_overlap = overlap
            best_ticker = ticker
    if best_ticker and best_overlap >= 1:
        logger.debug("Resolver: '{}' -> {} (static_map_word_overlap)".format(
            exposure_category, best_ticker))
        return best_ticker, "static_map_word_overlap"

    # Tier 2: DB cache
    conn = get_conn()
    cached = conn.execute(
        "SELECT proxy_ticker, resolve_method FROM resolver_cache WHERE exposure_category = ?",
        (normalised,)
    ).fetchone()
    conn.close()
    if cached:
        logger.debug("Resolver: '{}' -> {} (cached)".format(exposure_category, cached["proxy_ticker"]))
        return cached["proxy_ticker"], "cached"

    # Tier 3: Dynamic search via yfinance
    ticker = _try_yfinance_resolve(normalised)
    if ticker:
        _cache_resolution(normalised, ticker, "yf_search")
        return ticker, "yf_search"

    # Tier 3b: Alpha Vantage SYMBOL_SEARCH
    ticker = _try_av_search(normalised)
    if ticker:
        _cache_resolution(normalised, ticker, "av_search")
        return ticker, "av_search"

    logger.warning("Resolver: '{}' could not be resolved".format(exposure_category))
    return None, "failed"


def _try_yfinance_resolve(category):
    """Try to find a valid ETF for this category using yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        return None

    # Generate candidate ticker guesses based on common ETF naming
    candidates = _generate_etf_candidates(category)
    for candidate in candidates:
        try:
            t = yf.Ticker(candidate)
            info = t.info
            if info and info.get("regularMarketPrice"):
                logger.info("Resolver: yfinance found '{}' for category '{}'".format(
                    candidate, category))
                return candidate
        except Exception:
            continue
    return None


def _generate_etf_candidates(category):
    """Generate plausible ETF ticker candidates from a category name."""
    candidates = []
    words = category.lower().split()

    # Try common ETF prefixes
    if "oil" in words or "crude" in words or "petroleum" in words:
        candidates.extend(["USO", "BNO", "OIL", "XLE"])
    if "gold" in words:
        candidates.extend(["GLD", "IAU", "GDX"])
    if "silver" in words:
        candidates.extend(["SLV", "SIVR"])
    if "gas" in words or "natural gas" in category.lower():
        candidates.extend(["UNG", "BOIL"])
    if "airline" in words or "aviation" in words:
        candidates.extend(["JETS"])
    if "bank" in words:
        candidates.extend(["KBE", "XLF"])
    if "tech" in words or "technology" in words:
        candidates.extend(["QQQ", "XLK", "VGT"])
    if "health" in words or "medical" in words:
        candidates.extend(["XLV", "XBI"])
    if "real estate" in category.lower() or "reit" in words:
        candidates.extend(["VNQ", "IYR"])
    if "bond" in words or "treasury" in words or "rate" in words:
        candidates.extend(["TLT", "SHY", "AGG"])
    if "infla" in category.lower():
        candidates.extend(["TIP", "RINF"])

    return candidates


def _try_av_search(category):
    """Try Alpha Vantage SYMBOL_SEARCH as last resort."""
    if not ALPHA_VANTAGE_KEY:
        return None

    import requests
    try:
        keywords = category + " ETF"
        resp = requests.get(
            ALPHA_VANTAGE_BASE,
            params={
                "function": "SYMBOL_SEARCH",
                "keywords": keywords,
                "apikey": ALPHA_VANTAGE_KEY,
            },
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        matches = data.get("bestMatches", [])

        # Prefer ETFs (type 'ETF') with high match score
        for m in matches:
            if m.get("3. type") == "ETF":
                ticker = m.get("1. symbol", "")
                logger.info("Resolver: AV search found '{}' for category '{}'".format(
                    ticker, category))
                time.sleep(AV_RATE_LIMIT)
                return ticker

        # Fallback: take first equity match
        if matches:
            ticker = matches[0].get("1. symbol", "")
            logger.info("Resolver: AV search (non-ETF) found '{}' for category '{}'".format(
                ticker, category))
            time.sleep(AV_RATE_LIMIT)
            return ticker

    except Exception as e:
        logger.warning("AV SYMBOL_SEARCH failed for '{}': {}".format(category, e))

    return None


def _cache_resolution(category, ticker, method):
    """Cache a resolved category -> ticker mapping in the database."""
    conn = get_conn()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO resolver_cache
            (exposure_category, proxy_ticker, resolve_method, resolved_at, confidence)
            VALUES (?, ?, ?, ?, ?)
        """, (category.lower().strip(), ticker, method,
              datetime.now(timezone.utc).isoformat(), "medium"))
        conn.commit()
    except Exception as e:
        logger.warning("Failed to cache resolution: {}".format(e))
    finally:
        conn.close()


def _is_valid_ticker(ticker):
    """Quick check if a ticker is likely a real, liquid instrument.
    Rejects non-ticker strings like 'US 2Y', 'EMFX basket', '5Y5Y inflation swap'.
    Also validates price > $1 to avoid penny stocks and mismatches.
    """
    if not ticker or not isinstance(ticker, str):
        return False
    # Real tickers: 1-5 uppercase letters, no spaces, no digits in first char
    ticker = ticker.strip()
    if " " in ticker:
        return False
    if len(ticker) > 5 or len(ticker) < 1:
        return False
    if not ticker.isalpha():
        return False
    # Validate via yfinance that it has a real price > $1
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.info
        price = info.get("regularMarketPrice") or info.get("previousClose") or 0
        if price and price > 1:
            return True
        return False
    except Exception:
        return False


def resolve_all_cascade(cascade_map):
    """Resolve proxy tickers for all entries in a cascade map.
    Returns list of (cascade_entry, ticker, method) tuples.

    When cascade specifies instruments, validates they are real liquid tickers.
    Falls back to exposure_category resolution if instruments are invalid
    (e.g. 'US 2Y', 'EMFX basket', 'TIPS' which is a penny stock not the ETF).
    """
    results = []
    for entry in cascade_map:
        category = entry.get("exposure_category", "")
        instruments = entry.get("instruments", entry.get("candidate_instruments", []))

        found = False
        if instruments:
            for inst in instruments:
                ticker = inst if isinstance(inst, str) else inst.get("ticker", "")
                ticker = ticker.strip()
                if ticker and _is_valid_ticker(ticker):
                    results.append((entry, ticker, "cascade_specified"))
                    found = True
                    break
                else:
                    logger.debug("Resolver: cascade instrument '{}' is not a valid ticker, "
                                 "trying next or falling back".format(ticker))

        if not found:
            # Resolve from the exposure_category
            ticker, method = resolve_proxy(category)
            results.append((entry, ticker, method))

    return results


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) > 1:
        category = " ".join(sys.argv[1:])
        ticker, method = resolve_proxy(category)
        if ticker:
            print("{} -> {} ({})".format(category, ticker, method))
        else:
            print("{} -> UNRESOLVED".format(category))
    else:
        # Test a few categories
        test_categories = [
            "airlines", "energy equities", "chemicals", "inflation expectations",
            "gold", "crude oil", "shipping", "treasury bonds", "semiconductors",
            "agricultural commodities", "emerging markets",
        ]
        for cat in test_categories:
            ticker, method = resolve_proxy(cat)
            print("  {} -> {} ({})".format(cat, ticker or "???", method))
