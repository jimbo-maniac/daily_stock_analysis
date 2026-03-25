# -*- coding: utf-8 -*-
"""
Financial Modeling Prep (FMP) client.

Fetches TTM ratios and revenue YoY growth for equity tickers.
Results are cached in-process so each symbol is queried at most once per run.
All failures are non-fatal: a warning is logged and None is returned.
"""

import logging
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://financialmodelingprep.com/api"
_TIMEOUT = 10  # seconds per request

# ETFs explicitly excluded from FMP lookups
_ETF_BLACKLIST: frozenset = frozenset({"TIP", "KSA", "UAE", "UNG", "GLD", "PHYS"})


def is_fmp_eligible(symbol: str) -> bool:
    """Return True if *symbol* should be looked up in FMP."""
    upper = symbol.upper()
    if upper in _ETF_BLACKLIST:
        return False
    # BTC-USD is allowed despite containing a hyphen
    if upper == "BTC-USD":
        return True
    if "." in symbol or "-" in symbol:
        return False
    return True


class FMPClient:
    """Thin FMP REST client with per-run in-process caching."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._cache: Dict[str, Optional[Dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_fundamentals(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Return a dict with FMP fundamental fields for *symbol*, or None.

        Keys (all may be None if the API did not return them):
            pe_ttm, pb, dividend_yield, roe,
            last_year_revenue, prev_year_revenue, revenue_yoy_growth
        """
        if symbol in self._cache:
            return self._cache[symbol]

        if not is_fmp_eligible(symbol):
            logger.debug("[FMP] Skipping ineligible symbol: %s", symbol)
            self._cache[symbol] = None
            return None

        result = self._fetch(symbol)
        self._cache[symbol] = result
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            ratios = self._get_ratios_ttm(symbol)
            revenue = self._get_revenue_yoy(symbol)
            combined = {**ratios, **revenue}
            logger.info("[FMP] %s fundamentals: %s", symbol, combined)
            return combined
        except Exception as exc:
            logger.warning("[FMP] Failed to fetch fundamentals for %s: %s", symbol, exc)
            return None

    def _get_ratios_ttm(self, symbol: str) -> Dict[str, Any]:
        url = f"{_BASE_URL}/v3/ratios-ttm/{symbol}"
        try:
            resp = requests.get(url, params={"apikey": self._api_key}, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return {}
            row = data[0] if isinstance(data, list) else data
            return {
                "pe_ttm": row.get("peRatioTTM"),
                "pb": row.get("priceToBookRatioTTM"),
                "dividend_yield": row.get("dividendYieldTTM"),
                "roe": row.get("returnOnEquityTTM"),
            }
        except Exception as exc:
            logger.warning("[FMP] ratios-ttm failed for %s: %s", symbol, exc)
            return {}

    def _get_revenue_yoy(self, symbol: str) -> Dict[str, Any]:
        url = f"{_BASE_URL}/v3/income-statement/{symbol}"
        try:
            resp = requests.get(
                url,
                params={"limit": 2, "apikey": self._api_key},
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return {}
            last_rev = data[0].get("revenue") if len(data) >= 1 else None
            prev_rev = data[1].get("revenue") if len(data) >= 2 else None
            yoy: Optional[float] = None
            if last_rev is not None and prev_rev is not None and prev_rev != 0:
                yoy = round((last_rev - prev_rev) / abs(prev_rev) * 100, 2)
            return {
                "last_year_revenue": last_rev,
                "prev_year_revenue": prev_rev,
                "revenue_yoy_growth": yoy,
            }
        except Exception as exc:
            logger.warning("[FMP] income-statement failed for %s: %s", symbol, exc)
            return {}
