"""
NAS Signal Tester - Configuration
All constants, thresholds, ETF proxy map, and paths.
"""
import os

# RSS feeds for NAS signal_packs
# Replace with actual feed URL when NAS pipeline is live
RSS_FEEDS = [
    # "https://nassignal.makes.news/rss.xml",
]
RSS_URL = RSS_FEEDS[0] if RSS_FEEDS else ""
REPORT_TITLE_PREFIX = "NAS"

# Price data sources
# Primary: yfinance (no API key, no rate limits)
# Secondary: Alpha Vantage (backup)
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")
ALPHA_VANTAGE_BASE = "https://www.alphavantage.co/query"
AV_RATE_LIMIT = 12  # seconds between AV calls (5/min free tier)

# Scoring thresholds
MIN_MOVE_PCT = 0.5               # Minimum % move to count as a directional hit
HORIZONS = [6, 12, 24, 72]       # Hours at which to score positions
SAMPLE_INTERVAL_MINUTES = 30     # Price sampling granularity

# Signal selectivity thresholds
TRADEABLE_MIN_STRENGTH = "Moderate"  # Minimum signal_strength to count as tradeable
TRADEABLE_MIN_ACCEL = 5.0            # Minimum abs(acceleration_delta)

# Intervals (seconds)
SCAN_INTERVAL = 30 * 60          # Check RSS every 30 minutes
TRACK_INTERVAL = 30 * 60         # Fetch prices every 30 minutes
SCORE_INTERVAL = 60 * 60         # Recompute scores every hour
REPORT_INTERVAL = 6 * 60 * 60    # Heartbeat report every 6 hours

# Market hours (UTC) — for awareness, yfinance works anytime
MARKET_OPEN_UTC = 14.5   # 14:30
MARKET_CLOSE_UTC = 21.0  # 21:00
MARKET_DAYS = [0, 1, 2, 3, 4]  # Monday-Friday

# Position management
DEFAULT_DECAY_WINDOW_HOURS = 72
MAX_MONITORING_HOURS = 168       # 7 days max
TRACKING_WINDOW_HOURS = 168
ACCEL_REFRESH_THRESHOLD = 15.0   # Refresh T0 if acceleration_delta jumps by this much

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
DB_PATH = os.path.join(DATA_DIR, "nas_tester.db")

# ETF Proxy Map — maps exposure_category strings to liquid ETF tickers
# Used by resolver.py for Tier 1 (static) lookups
ETF_PROXY_MAP = {
    # Energy
    "energy equities": "XLE",
    "oil & gas": "XOP",
    "oil & gas exploration": "XOP",
    "oil majors": "XLE",
    "crude oil": "USO",
    "oil": "USO",
    "brent crude": "BNO",
    "natural gas": "UNG",
    "clean energy": "ICLN",
    "nuclear energy": "URA",
    "solar": "TAN",

    # Airlines / Transport
    "airlines": "JETS",
    "shipping": "SLX",
    "logistics": "IYT",
    "transport": "IYT",
    "transportation": "IYT",

    # Metals & Mining
    "gold": "GLD",
    "silver": "SLV",
    "copper": "COPX",
    "platinum": "PPLT",
    "palladium": "PALL",
    "lithium": "LIT",
    "steel": "SLX",
    "rare earths": "REMX",
    "mining": "XME",
    "metals": "XME",

    # Agriculture
    "agricultural commodities": "DBA",
    "agriculture": "DBA",
    "soft commodities": "DBA",
    "wheat": "WEAT",
    "corn": "CORN",
    "soybeans": "SOYB",

    # Broad commodities
    "broad commodities": "DJP",
    "commodities": "DJP",

    # Sectors
    "chemicals": "XLB",
    "materials": "XLB",
    "defense": "ITA",
    "defence": "ITA",
    "aerospace": "ITA",
    "semiconductors": "SMH",
    "tech": "QQQ",
    "technology": "QQQ",
    "financials": "XLF",
    "banks": "KBE",
    "insurance": "KIE",
    "healthcare": "XLV",
    "pharma": "XBI",
    "biotech": "IBB",
    "utilities": "XLU",
    "real estate": "VNQ",
    "consumer staples": "XLP",
    "consumer discretionary": "XLY",
    "retail": "XRT",
    "industrials": "XLI",

    # Geographies
    "china tech": "KWEB",
    "china": "FXI",
    "china broad": "FXI",
    "india": "INDA",
    "japan": "EWJ",
    "europe": "VGK",
    "emerging markets": "EEM",
    "uk equities": "EWU",
    "brazil": "EWZ",
    "south korea": "EWY",

    # Macro / Indices
    "us equities": "SPY",
    "s&p 500": "SPY",
    "broad index": "SPY",
    "nasdaq": "QQQ",
    "small cap": "IWM",
    "dow jones": "DIA",

    # Fixed income
    "treasury bonds": "TLT",
    "long bonds": "TLT",
    "short-term bonds": "SHY",
    "corporate bonds": "LQD",
    "high yield bonds": "HYG",
    "inflation expectations": "TIP",
    "tips": "TIP",
    "rates": "TLT",

    # Volatility / FX
    "volatility": "VXX",
    "vix": "VXX",
    "us dollar": "UUP",
    "dollar": "UUP",
    "euro": "FXE",
    "yen": "FXY",

    # Cross-asset common cascade categories
    "travel & tourism": "JETS",
    "food producers": "DBA",
    "plastics": "XLB",
}

# Strength ordering for signal_strength comparisons
STRENGTH_ORDER = {"Low": 0, "Moderate": 1, "High": 2}
