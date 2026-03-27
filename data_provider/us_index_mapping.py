# -*- coding: utf-8 -*-
"""
===================================
Global market symbol routing utilities
===================================

Provides:
1. US index code mapping (e.g. SPX -> ^GSPC)
2. US stock code identification (AAPL, TSLA etc.)
3. European exchange suffix detection (.DE, .AS, .L, .PA, .OL, .CO)
4. Crypto pair detection (BTC-USD, ETH-USD)
5. Global index mapping for macro dashboard
"""

import re


# US stock code pattern: 1-5 uppercase letters, optional .X suffix (e.g. BRK.B)
_US_STOCK_PATTERN = re.compile(r'^[A-Z]{1,5}(\.[A-Z])?$')

# European exchange suffixes recognized by yfinance
EUROPEAN_EXCHANGE_SUFFIXES = frozenset({
    '.DE',   # Frankfurt / XETRA
    '.AS',   # Amsterdam (Euronext)
    '.PA',   # Paris (Euronext)
    '.L',    # London Stock Exchange
    '.OL',   # Oslo Bors
    '.CO',   # Copenhagen (Nasdaq Nordic)
    '.HE',   # Helsinki (Nasdaq Nordic)
    '.ST',   # Stockholm (Nasdaq Nordic)
    '.MI',   # Milan (Borsa Italiana)
    '.MC',   # Madrid
    '.SW',   # SIX Swiss Exchange
    '.BR',   # Brussels (Euronext)
    '.LS',   # Lisbon (Euronext)
    '.VI',   # Vienna
    '.IR',   # Dublin (Euronext)
})

# Crypto pair pattern: e.g. BTC-USD, ETH-USD, SOL-USD
_CRYPTO_PATTERN = re.compile(r'^[A-Z]{2,10}-[A-Z]{3,4}$')

# FX pair pattern: e.g. EURUSD=X
_FX_PATTERN = re.compile(r'^[A-Z]{6}=X$')


# User input -> (Yahoo Finance symbol, display name)
US_INDEX_MAPPING = {
    # S&P 500
    'SPX': ('^GSPC', 'S&P 500'),
    '^GSPC': ('^GSPC', 'S&P 500'),
    'GSPC': ('^GSPC', 'S&P 500'),
    # Dow Jones Industrial Average
    'DJI': ('^DJI', 'Dow Jones'),
    '^DJI': ('^DJI', 'Dow Jones'),
    'DJIA': ('^DJI', 'Dow Jones'),
    # NASDAQ Composite
    'IXIC': ('^IXIC', 'NASDAQ Composite'),
    '^IXIC': ('^IXIC', 'NASDAQ Composite'),
    'NASDAQ': ('^IXIC', 'NASDAQ Composite'),
    # NASDAQ 100
    'NDX': ('^NDX', 'NASDAQ 100'),
    '^NDX': ('^NDX', 'NASDAQ 100'),
    # VIX Volatility Index
    'VIX': ('^VIX', 'VIX'),
    '^VIX': ('^VIX', 'VIX'),
    # Russell 2000
    'RUT': ('^RUT', 'Russell 2000'),
    '^RUT': ('^RUT', 'Russell 2000'),
}

# Global index mapping for macro dashboard
GLOBAL_INDEX_MAPPING = {
    # US
    'SPX': ('^GSPC', 'S&P 500'),
    # Europe
    'STOXX50': ('^STOXX50E', 'EURO STOXX 50'),
    'DAX': ('^GDAXI', 'DAX'),
    'FTSE': ('^FTSE', 'FTSE 100'),
    # Asia
    'NIKKEI': ('^N225', 'Nikkei 225'),
    'HSI': ('^HSI', 'Hang Seng'),
    # Commodities / Macro
    'GOLD': ('GC=F', 'Gold Futures'),
    'BRENT': ('BZ=F', 'Brent Crude'),
    'VIX': ('^VIX', 'VIX'),
    'EURUSD': ('EURUSD=X', 'EUR/USD'),
    'BTC': ('BTC-USD', 'Bitcoin'),
}


def is_us_index_code(code: str) -> bool:
    """Check if code is a known US index symbol."""
    return (code or '').strip().upper() in US_INDEX_MAPPING


def is_european_ticker(code: str) -> bool:
    """Check if code has a European exchange suffix (.DE, .AS, .L, .PA, .OL etc.)."""
    normalized = (code or '').strip().upper()
    if '.' not in normalized:
        return False
    dot_pos = normalized.rfind('.')
    suffix = normalized[dot_pos:]
    return suffix in EUROPEAN_EXCHANGE_SUFFIXES


def is_crypto_pair(code: str) -> bool:
    """Check if code is a crypto pair (e.g. BTC-USD, ETH-USD)."""
    normalized = (code or '').strip().upper()
    return bool(_CRYPTO_PATTERN.match(normalized))


def is_fx_pair(code: str) -> bool:
    """Check if code is an FX pair (e.g. EURUSD=X)."""
    normalized = (code or '').strip().upper()
    return bool(_FX_PATTERN.match(normalized))


def is_us_stock_code(code: str) -> bool:
    """
    Check if code is a US stock symbol (excluding US indices, EU tickers, crypto, FX).

    US stock codes are 1-5 uppercase letters, optional .X suffix (e.g. BRK.B).
    Explicitly excluded: US indices, European exchange suffixes, crypto pairs, FX pairs.
    """
    normalized = (code or '').strip().upper()
    if normalized in US_INDEX_MAPPING:
        return False
    if is_european_ticker(normalized):
        return False
    if is_crypto_pair(normalized):
        return False
    if is_fx_pair(normalized):
        return False
    return bool(_US_STOCK_PATTERN.match(normalized))


def get_us_index_yf_symbol(code: str) -> tuple:
    """
    Get US index Yahoo Finance symbol and display name.

    Returns:
        (yf_symbol, display_name) tuple, or (None, None) if not found.
    """
    normalized = (code or '').strip().upper()
    return US_INDEX_MAPPING.get(normalized, (None, None))


def get_global_index_yf_symbol(key: str) -> tuple:
    """
    Get global index/macro Yahoo Finance symbol and display name.

    Returns:
        (yf_symbol, display_name) tuple, or (None, None) if not found.
    """
    normalized = (key or '').strip().upper()
    return GLOBAL_INDEX_MAPPING.get(normalized, (None, None))


def get_asset_class(code: str) -> str:
    """
    Determine the asset class for a given ticker.

    Returns one of: 'us_index', 'us_stock', 'eu_stock', 'hk_stock', 'crypto', 'fx', 'cn_stock'
    """
    normalized = (code or '').strip().upper()
    if normalized in US_INDEX_MAPPING:
        return 'us_index'
    if is_crypto_pair(normalized):
        return 'crypto'
    if is_fx_pair(normalized):
        return 'fx'
    if is_european_ticker(normalized):
        return 'eu_stock'
    # HK detection delegated to base.py _is_hk_market
    if normalized.startswith('HK') or normalized.endswith('.HK'):
        return 'hk_stock'
    if bool(_US_STOCK_PATTERN.match(normalized)):
        return 'us_stock'
    return 'cn_stock'
