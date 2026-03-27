# -*- coding: utf-8 -*-
"""
market review regionconfiguration

defineeachmarketzonedomainindex、newssearchword、Prompt Tipetcyuandata，
provide MarketAnalyzer by region switch A stocks/US stockreviewrowas。
"""

from dataclasses import dataclass
from typing import List


@dataclass
class MarketProfile:
    """market review regionconfiguration"""

    region: str  # "cn" | "us"
    # fordetermineoveralltrendindexcode，cn useShanghai Composite 000001，us useS&P SPX
    mood_index_code: str
    # newssearchkeyword
    news_queries: List[str]
    # indexpointevaluate Prompt Tiplanguage
    prompt_index_hint: str
    # market overviewwhetherpackageincludeprice changecountcount、limit uplimit down（A stockshas，US stockno）
    has_market_stats: bool
    # market overviewwhetherpackageincludesectorprice change（A stockshas，US stocktemporarilyno）
    has_sector_rankings: bool


CN_PROFILE = MarketProfile(
    region="cn",
    mood_index_code="000001",
    news_queries=[
        "A-share market index review",
        "stocksmarket quote/market data analyzing",
        "A-share market hotspot sector",
    ],
    prompt_index_hint="analyzingShanghai Composite、Shenzhen、ChiNextetceachindextrendfeatures",
    has_market_stats=True,
    has_sector_rankings=True,
)

US_PROFILE = MarketProfile(
    region="us",
    mood_index_code="SPX",
    news_queries=[
        "US stock market index",
        "US stock market",
        "S&P 500 NASDAQ",
    ],
    prompt_index_hint="Analyze S&P 500, NASDAQ, Dow Jones trend features",
    has_market_stats=False,
    has_sector_rankings=False,
)

GLOBAL_PROFILE = MarketProfile(
    region="global",
    mood_index_code="SPX",
    news_queries=[
        "global macro market outlook",
        "European stock market DAX STOXX",
        "gold oil commodities geopolitical risk",
        "inflation central bank policy rates",
        "S&P 500 NASDAQ market",
    ],
    prompt_index_hint=(
        "Analyze S&P 500, EURO STOXX 50, DAX, FTSE, Gold, Brent, VIX, "
        "EUR/USD, and Bitcoin as a connected macro dashboard"
    ),
    has_market_stats=False,
    has_sector_rankings=False,
)


def get_profile(region: str) -> MarketProfile:
    """Return MarketProfile for the given region."""
    if region == "us":
        return US_PROFILE
    if region in ("global", "eu"):
        return GLOBAL_PROFILE
    return CN_PROFILE
