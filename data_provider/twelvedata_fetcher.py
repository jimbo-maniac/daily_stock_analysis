# -*- coding: utf-8 -*-
"""
TwelveDataFetcher - Fallback data source (Priority 5)
=====================================================

Data source: Twelve Data REST API (https://twelvedata.com)
Purpose: Fallback for FX and crypto symbols that yfinance cannot handle.

Key strategy:
- Priority 5 — only tried when all lower-numbered sources (including yfinance) fail.
- Requires TWELVE_DATA_API_KEY environment variable.
- Converts internal code conventions (BTC-USD → BTC/USD) before the API call.
- Reuses BaseFetcher._calculate_indicators() to derive MA5/10/20 identically.
"""

import logging
import os
from typing import Optional

import pandas as pd
import requests

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.twelvedata.com"
_TIMEOUT = 20  # seconds


def _to_twelvedata_symbol(code: str) -> str:
    """Convert an internal stock/crypto/FX code to Twelve Data symbol format."""
    upper = code.strip().upper()
    # Crypto: BTC-USD → BTC/USD
    if upper.endswith("-USD"):
        return upper[:-4] + "/USD"
    # Generic 7-char FX pair with hyphen: EUR-USD → EUR/USD
    if len(upper) == 7 and upper[3] == "-":
        return upper[:3] + "/" + upper[4:]
    return upper


class TwelveDataFetcher(BaseFetcher):
    """
    Twelve Data fallback fetcher.

    Priority: 5 (only used when yfinance and all other sources fail).
    Reads TWELVE_DATA_API_KEY from the environment at instantiation time.
    """

    name = "TwelveDataFetcher"
    priority = 5

    def __init__(self) -> None:
        self._api_key: str = os.getenv("TWELVE_DATA_API_KEY", "").strip()

    @property
    def is_available(self) -> bool:
        return bool(self._api_key)

    # ------------------------------------------------------------------
    # BaseFetcher abstract methods
    # ------------------------------------------------------------------

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Call the Twelve Data /time_series endpoint and return a raw DataFrame."""
        if not self._api_key:
            raise DataFetchError("TWELVE_DATA_API_KEY is not configured")

        symbol = _to_twelvedata_symbol(stock_code)
        params = {
            "symbol": symbol,
            "interval": "1day",
            "start_date": start_date,
            "end_date": end_date,
            "apikey": self._api_key,
            "order": "ASC",
            "format": "JSON",
        }

        try:
            resp = requests.get(f"{_BASE_URL}/time_series", params=params, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.Timeout as exc:
            raise DataFetchError(f"Twelve Data request timed out for {symbol}") from exc
        except Exception as exc:
            raise DataFetchError(f"Twelve Data request failed for {symbol}: {exc}") from exc

        # API-level error (e.g. invalid symbol, quota exceeded)
        if data.get("status") not in (None, "ok"):
            msg = data.get("message") or data.get("code") or "unknown error"
            raise DataFetchError(f"Twelve Data API error for {symbol}: {msg}")

        values = data.get("values")
        if not values:
            raise DataFetchError(f"Twelve Data returned no values for {symbol}")

        logger.debug("[TwelveData] %s: received %d rows", symbol, len(values))
        return pd.DataFrame(values)

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """Map Twelve Data column names to the pipeline's STANDARD_COLUMNS."""
        df = df.copy()

        df = df.rename(columns={"datetime": "date"})

        # Convert all price/volume columns to float
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # pct_chg — Twelve Data does not provide this
        if "close" in df.columns:
            df["pct_chg"] = df["close"].pct_change() * 100
            df["pct_chg"] = df["pct_chg"].fillna(0).round(2)

        # amount — approximate as volume × close
        if "volume" in df.columns and "close" in df.columns:
            df["amount"] = df["volume"] * df["close"]
        else:
            df["amount"] = 0.0

        df["code"] = stock_code

        keep_cols = ["code"] + STANDARD_COLUMNS
        return df[[c for c in keep_cols if c in df.columns]]
