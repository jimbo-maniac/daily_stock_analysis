# -*- coding: utf-8 -*-
"""
===================================
datasourcestrategylayer - packageinitializing
===================================

thispackageimplementstrategymodemanagemultiplecountdatasource，implement：
1. unifieddatagetAPI/interface
2. automatic failover
3. preventblockprohibitstreamcontrolstrategy

datasource priority（dynamicadjust）：
【configuration TUSHARE_TOKEN when】
1. TushareFetcher (Priority 0) - 🔥 highest priority（dynamicimprove）
2. EfinanceFetcher (Priority 0) - samepriority
3. AkshareFetcher (Priority 1) - from akshare library
4. PytdxFetcher (Priority 2) - from pytdx library（TongDaXin）
5. BaostockFetcher (Priority 3) - from baostock library
6. YfinanceFetcher (Priority 4) - from yfinance library

【notconfiguration TUSHARE_TOKEN when】
1. EfinanceFetcher (Priority 0) - highest priority，from efinance library
2. AkshareFetcher (Priority 1) - from akshare library
3. PytdxFetcher (Priority 2) - from pytdx library（TongDaXin）
4. TushareFetcher (Priority 2) - from tushare library（unavailable）
5. BaostockFetcher (Priority 3) - from baostock library
6. YfinanceFetcher (Priority 4) - from yfinance library

Tip：prioritycountcharactermoresmallmorepriority，sameprioritybyinitializingorderarrange
"""

from .base import BaseFetcher, DataFetcherManager
from .efinance_fetcher import EfinanceFetcher
from .akshare_fetcher import AkshareFetcher, is_hk_stock_code
from .tushare_fetcher import TushareFetcher
from .pytdx_fetcher import PytdxFetcher
from .baostock_fetcher import BaostockFetcher
from .yfinance_fetcher import YfinanceFetcher
from .us_index_mapping import (
    is_us_index_code,
    is_us_stock_code,
    is_european_ticker,
    is_crypto_pair,
    is_fx_pair,
    get_us_index_yf_symbol,
    get_asset_class,
    US_INDEX_MAPPING,
    GLOBAL_INDEX_MAPPING,
)

__all__ = [
    'BaseFetcher',
    'DataFetcherManager',
    'EfinanceFetcher',
    'AkshareFetcher',
    'TushareFetcher',
    'PytdxFetcher',
    'BaostockFetcher',
    'YfinanceFetcher',
    'is_us_index_code',
    'is_us_stock_code',
    'is_european_ticker',
    'is_crypto_pair',
    'is_fx_pair',
    'is_hk_stock_code',
    'get_us_index_yf_symbol',
    'get_asset_class',
    'US_INDEX_MAPPING',
    'GLOBAL_INDEX_MAPPING',
]
