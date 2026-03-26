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
    prompt_index_hint="analyzingS&P500、NASDAQ、pathrefers toetceachindextrendfeatures",
    has_market_stats=False,
    has_sector_rankings=False,
)


def get_profile(region: str) -> MarketProfile:
    """based on region returntoshould MarketProfile"""
    if region == "us":
        return US_PROFILE
    return CN_PROFILE
