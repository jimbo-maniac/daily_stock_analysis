# -*- coding: utf-8 -*-
"""
===================================
trading calendarmodule (Issue #373)
===================================

Responsibilities:
1. bymarket（A-share/HK stock/US stock）determinetodaywhether istrading day
2. bymarketwhenzoneget“today”date，avoidservicehandler UTC causedateerror
3. support per-stock filtering：onlyanalyzingtodayopenmarketmarketstock

dependency：exchange-calendars（optional，unavailablewhen fail-open）
"""

import logging
from datetime import date, datetime
from typing import Optional, Set

logger = logging.getLogger(__name__)

# Exchange-calendars availability
_XCALS_AVAILABLE = False
try:
    import exchange_calendars as xcals
    _XCALS_AVAILABLE = True
except ImportError:
    logger.warning(
        "exchange-calendars not installed; trading day check disabled. "
        "Run: pip install exchange-calendars"
    )

# Market -> exchange code (exchange-calendars)
MARKET_EXCHANGE = {
    "cn": "XSHG",
    "hk": "XHKG",
    "us": "XNYS",
    "eu": "XAMS",   # Amsterdam (Euronext) as proxy for EU trading days
    "crypto": None,  # Crypto trades 24/7
    "fx": None,      # FX trades 24/5
}

# Market -> IANA timezone for "today"
MARKET_TIMEZONE = {
    "cn": "Asia/Shanghai",
    "hk": "Asia/Hong_Kong",
    "us": "America/New_York",
    "eu": "Europe/Amsterdam",
    "crypto": "UTC",
    "fx": "UTC",
}


def get_market_for_stock(code: str) -> Optional[str]:
    """
    Infer market region for a stock code.

    Returns:
        'cn' | 'hk' | 'us' | 'eu' | 'crypto' | 'fx' | None
        (None = unrecognized, fail-open: treat as open)
    """
    if not code or not isinstance(code, str):
        return None
    code = (code or "").strip().upper()

    from data_provider.us_index_mapping import (
        is_us_stock_code, is_us_index_code,
        is_european_ticker, is_crypto_pair, is_fx_pair,
    )
    from data_provider.base import _is_hk_market

    if is_crypto_pair(code):
        return "crypto"
    if is_fx_pair(code):
        return "fx"
    if is_european_ticker(code):
        return "eu"
    if is_us_stock_code(code) or is_us_index_code(code):
        return "us"
    if _is_hk_market(code):
        return "hk"
    # A-share: 6-digit numeric
    if code.isdigit() and len(code) == 6:
        return "cn"
    return None


def is_market_open(market: str, check_date: date) -> bool:
    """
    Check if the given market is open on the given date.

    Fail-open: returns True if exchange-calendars unavailable or date out of range.

    Args:
        market: 'cn' | 'hk' | 'us'
        check_date: Date to check

    Returns:
        True if trading day (or fail-open), False otherwise
    """
    # Crypto and FX trade 24/7 (or 24/5 for FX)
    if market in ("crypto", "fx"):
        return True
    if not _XCALS_AVAILABLE:
        return True
    ex = MARKET_EXCHANGE.get(market)
    if not ex:
        return True
    try:
        cal = xcals.get_calendar(ex)
        session = datetime(check_date.year, check_date.month, check_date.day)
        return cal.is_session(session)
    except Exception as e:
        logger.warning("trading_calendar.is_market_open fail-open: %s", e)
        return True


def get_open_markets_today() -> Set[str]:
    """
    Get markets that are open today (by each market's local timezone).

    Returns:
        Set of market keys ('cn', 'hk', 'us') that are trading today
    """
    if not _XCALS_AVAILABLE:
        return {"cn", "hk", "us", "eu", "crypto", "fx"}
    result: Set[str] = set()
    from zoneinfo import ZoneInfo
    for mkt, tz_name in MARKET_TIMEZONE.items():
        try:
            tz = ZoneInfo(tz_name)
            today = datetime.now(tz).date()
            if is_market_open(mkt, today):
                result.add(mkt)
        except Exception as e:
            logger.warning("get_open_markets_today fail-open for %s: %s", mkt, e)
            result.add(mkt)
    return result


def compute_effective_region(
    config_region: str, open_markets: Set[str]
) -> Optional[str]:
    """
    Compute effective market review region given config and open markets.

    Args:
        config_region: From MARKET_REVIEW_REGION ('cn' | 'us' | 'both')
        open_markets: Markets open today

    Returns:
        None: caller uses config default (check disabled)
        '': all relevant markets closed, skip market review
        'cn' | 'us' | 'both': effective subset for today
    """
    if config_region not in ("cn", "us", "eu", "global", "both"):
        config_region = "global"
    if config_region == "cn":
        return "cn" if "cn" in open_markets else ""
    if config_region == "us":
        return "us" if "us" in open_markets else ""
    if config_region == "eu":
        return "eu" if "eu" in open_markets else ""
    # global or both: check if any relevant market is open
    relevant = {"us", "eu", "crypto"}  # Core markets for global portfolio
    active = relevant & open_markets
    if not active:
        return ""
    return "global"
