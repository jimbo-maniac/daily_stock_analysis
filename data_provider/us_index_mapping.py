# -*- coding: utf-8 -*-
"""
===================================
US stockindexwithstock codetool
===================================

provide：
1. US stockindexcodemapping（e.g. SPX -> ^GSPC）
2. US stockstock codeidentify（AAPL、TSLA etc）

US stockindexin Yahoo Finance inneeduse ^ prefix，withstock codenotsame。
"""

import re

# US stockcodepositivethen：1-5 uppercase letters，optional .X suffix（e.g. BRK.B）
_US_STOCK_PATTERN = re.compile(r'^[A-Z]{1,5}(\.[A-Z])?$')


# userinput -> (Yahoo Finance symbol, Chinesename)
US_INDEX_MAPPING = {
    # S&P 500
    'SPX': ('^GSPC', 'S&P500index'),
    '^GSPC': ('^GSPC', 'S&P500index'),
    'GSPC': ('^GSPC', 'S&P500index'),
    # Dow Jones Industrialaverageindex
    'DJI': ('^DJI', 'Dow Jones Industrialindex'),
    '^DJI': ('^DJI', 'Dow Jones Industrialindex'),
    'DJIA': ('^DJI', 'Dow Jones Industrialindex'),
    # NASDAQ Compositeindex
    'IXIC': ('^IXIC', 'NASDAQ Compositeindex'),
    '^IXIC': ('^IXIC', 'NASDAQ Compositeindex'),
    'NASDAQ': ('^IXIC', 'NASDAQ Compositeindex'),
    # NASDAQ 100
    'NDX': ('^NDX', 'NASDAQ100index'),
    '^NDX': ('^NDX', 'NASDAQ100index'),
    # VIX volatilityindex
    'VIX': ('^VIX', 'VIXpanicindex'),
    '^VIX': ('^VIX', 'VIXpanicindex'),
    # Russell 2000
    'RUT': ('^RUT', 'Russell2000index'),
    '^RUT': ('^RUT', 'Russell2000index'),
}


def is_us_index_code(code: str) -> bool:
    """
    check if code isUS stockindexsymbol。

    Args:
        code: stock/indexcode，e.g. 'SPX', 'DJI'

    Returns:
        True indicatesalreadyknowUS stockindexsymbol，otherwise False

    Examples:
        >>> is_us_index_code('SPX')
        True
        >>> is_us_index_code('AAPL')
        False
    """
    return (code or '').strip().upper() in US_INDEX_MAPPING


def is_us_stock_code(code: str) -> bool:
    """
    check if code isUS stockstocksymbol（excludeUS stockindex）。

    US stockstock codeas 1-5 uppercase letters，optional .X suffixe.g. BRK.B。
    US stockindex（SPX、DJI etc）cleardetermineexclude。

    Args:
        code: stock code，e.g. 'AAPL', 'TSLA', 'BRK.B'

    Returns:
        True indicatesUS stockstocksymbol，otherwise False

    Examples:
        >>> is_us_stock_code('AAPL')
        True
        >>> is_us_stock_code('TSLA')
        True
        >>> is_us_stock_code('BRK.B')
        True
        >>> is_us_stock_code('SPX')
        False
        >>> is_us_stock_code('600519')
        False
    """
    normalized = (code or '').strip().upper()
    # US stockindexis notstock
    if normalized in US_INDEX_MAPPING:
        return False
    return bool(_US_STOCK_PATTERN.match(normalized))


def get_us_index_yf_symbol(code: str) -> tuple:
    """
    getUS stockindex Yahoo Finance symbolwithChinesename。

    Args:
        code: userinput，e.g. 'SPX', '^GSPC', 'DJI'

    Returns:
        (yf_symbol, chinese_name) tuple，not foundreturn when (None, None)。

    Examples:
        >>> get_us_index_yf_symbol('SPX')
        ('^GSPC', 'S&P500index')
        >>> get_us_index_yf_symbol('AAPL')
        (None, None)
    """
    normalized = (code or '').strip().upper()
    return US_INDEX_MAPPING.get(normalized, (None, None))
