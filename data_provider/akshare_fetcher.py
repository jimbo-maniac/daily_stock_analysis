# -*- coding: utf-8 -*-
"""
===================================
AkshareFetcher - Primary data source (Priority 1)
===================================

Data sources:
1. Eastmoney crawler (via akshare library) - default data source
2. Sina Finance interface - alternative data source
3. Tencent Finance interface - alternative data source

Features: free, no token required, comprehensive data
Risk: crawler mechanism may be blocked by anti-scraping

Anti-blocking strategy:
1. Random sleep 2-5 seconds before each request
2. Randomly rotate User-Agent
3. Use tenacity for exponential backoff retry
4. Circuit breaker mechanism: automatic cooldown after consecutive failures

Enhanced data:
- Real-time quotes: volume ratio, turnover rate, P/E ratio, P/B ratio, total market cap, circulating market cap
- Chip distribution: profit ratio, average cost, chip concentration
"""

import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from patch.eastmoney_patch import eastmoney_patch
from src.config import get_config
from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS, is_bse_code, is_st_stock, is_kc_cy_stock, normalize_stock_code
from .realtime_types import (
    UnifiedRealtimeQuote, ChipDistribution, RealtimeSource,
    get_realtime_circuit_breaker, get_chip_circuit_breaker,
    safe_float, safe_int  # Use unified type conversion functions
)
from .us_index_mapping import is_us_index_code, is_us_stock_code


# Keep old RealtimeQuote alias for backward compatibility
RealtimeQuote = UnifiedRealtimeQuote


logger = logging.getLogger(__name__)

SINA_REALTIME_ENDPOINT = "hq.sinajs.cn/list"
TENCENT_REALTIME_ENDPOINT = "qt.gtimg.cn/q"


# User-Agent pool for random rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]


# Cache real-time quote data (avoid duplicate requests)
# TTL set to 20 minutes (1200 seconds):
# - Batch analysis scenario: typically 30 stocks analyzed within 5 minutes, 20 minutes is sufficient
# - Real-time requirement: stock analysis doesn't need second-level real-time data, 20-minute delay is acceptable
# - Anti-blocking: reduce API call frequency
_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 1200  # 20-minute cache validity
}

# ETF real-time quote cache
_etf_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 1200  # 20-minute cache validity
}


def _is_etf_code(stock_code: str) -> bool:
    """
    Check if code is an ETF fund
    
    ETF code rules:
    - Shanghai Stock Exchange ETF: 51xxxx, 52xxxx, 56xxxx, 58xxxx
    - Shenzhen Stock Exchange ETF: 15xxxx, 16xxxx, 18xxxx
    
    Args:
        stock_code: stock/fundcode
        
    Returns:
        True indicates ETF code，False indicatesnormalstock code
    """
    etf_prefixes = ('51', '52', '56', '58', '15', '16', '18')
    code = stock_code.strip().split('.')[0]
    return code.startswith(etf_prefixes) and len(code) == 6


def _is_hk_code(stock_code: str) -> bool:
    """
    check if code isHK stock

    HK stockcode rules：
    - 5digit numbercode，e.g. '00700' (Tencent Holdings)
    - partialHK stockcodepossiblywithhasprefix，e.g. 'hk00700', 'hk1810'

    Args:
        stock_code: stock code

    Returns:
        True indicatesHK stockcode，False indicatesis notHK stockcode
    """
    # removepossibly 'hk' prefixandcheckwhether ispurecountcharacter
    code = stock_code.strip().lower()
    if code.endswith('.hk'):
        numeric_part = code[:-3]
        return numeric_part.isdigit() and 1 <= len(numeric_part) <= 5
    if code.startswith('hk'):
        # with hk prefixonefixedisHK stock，removedropprefixaftershouldaspurecountcharacter（1-5digit）
        numeric_part = code[2:]
        return numeric_part.isdigit() and 1 <= len(numeric_part) <= 5
    # noprefixwhen，5digitpurecountcharacteronly thenview asasHK stock（avoidmisjudge A stockscode）
    return code.isdigit() and len(code) == 5


def is_hk_stock_code(stock_code: str) -> bool:
    """
    Public API: determine if a stock code is a Hong Kong stock.

    Delegates to _is_hk_code for internal compatibility.

    Args:
        stock_code: Stock code (e.g. '00700', 'hk00700')

    Returns:
        True if HK stock, False otherwise
    """
    return _is_hk_code(stock_code)


def _is_us_code(stock_code: str) -> bool:
    """
    check if code isUS stockstock（notpackagebracketUS stockindex）。

    delegate to us_index_mapping module is_us_stock_code()。

    Args:
        stock_code: stock code

    Returns:
        True indicatesUS stockcode，False indicatesis notUS stockcode

    Examples:
        >>> _is_us_code('AAPL')
        True
        >>> _is_us_code('TSLA')
        True
        >>> _is_us_code('SPX')
        False
        >>> _is_us_code('600519')
        False
    """
    return is_us_stock_code(stock_code)


def _to_sina_tx_symbol(stock_code: str) -> str:
    """Convert 6-digit A-share code to sh/sz/bj prefixed symbol for Sina/Tencent APIs."""
    base = (stock_code.strip().split(".")[0] if "." in stock_code else stock_code).strip()
    if is_bse_code(base):
        return f"bj{base}"
    # Shanghai: 60xxxx, 5xxxx (ETF), 90xxxx (B-shares)
    if base.startswith(("6", "5", "90")):
        return f"sh{base}"
    return f"sz{base}"


def _classify_realtime_http_error(exc: Exception) -> Tuple[str, str]:
    """
    Classify Sina/Tencent realtime quote failures into stable categories.
    """
    detail = str(exc).strip() or type(exc).__name__
    lowered = detail.lower()

    remote_disconnect_keywords = (
        "remotedisconnected",
        "remote end closed connection without response",
        "connection aborted",
        "connection broken",
        "protocolerror",
        "chunkedencodingerror",
    )
    timeout_keywords = (
        "timeout",
        "timed out",
        "readtimeout",
        "connecttimeout",
    )
    rate_limit_keywords = (
        "banned",
        "blocked",
        "frequency",
        "rate limit",
        "too many requests",
        "429",
        "constraint",
        "forbidden",
        "403",
    )

    if any(keyword in lowered for keyword in remote_disconnect_keywords):
        return "remote_disconnect", detail
    if isinstance(exc, (TimeoutError, requests.exceptions.Timeout)) or any(
        keyword in lowered for keyword in timeout_keywords
    ):
        return "timeout", detail
    if any(keyword in lowered for keyword in rate_limit_keywords):
        return "rate_limit_or_anti_bot", detail
    if isinstance(exc, requests.exceptions.RequestException):
        return "request_error", detail
    return "unknown_request_error", detail


def _build_realtime_failure_message(
    source_name: str,
    endpoint: str,
    stock_code: str,
    symbol: str,
    category: str,
    detail: str,
    elapsed: float,
    error_type: str,
) -> str:
    return (
        f"{source_name} realtimequote/market dataAPI/interfacefailed: endpoint={endpoint}, stock_code={stock_code}, "
        f"symbol={symbol}, category={category}, error_type={error_type}, "
        f"elapsed={elapsed:.2f}s, detail={detail}"
    )


class AkshareFetcher(BaseFetcher):
    """
    Akshare datasource implementation
    
    priority：1（highest）
    Data sources:Eastmoneyweb crawler
    
    key strategy：
    - each timerequestrandom sleep before 2.0-5.0 seconds
    - random User-Agent rotate
    - failedafterindexbackoffretry（at most3times）
    """
    
    name = "AkshareFetcher"
    priority = int(os.getenv("AKSHARE_PRIORITY", "1"))
    
    def __init__(self, sleep_min: float = 2.0, sleep_max: float = 5.0):
        """
        initializing AkshareFetcher
        
        Args:
            sleep_min: mostsmallsleeptime（seconds）
            sleep_max: maxsleeptime（seconds）
        """
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self._last_request_time: Optional[float] = None
        # Eastmoneypatch must be enabledexecuteapply patchoperation
        if get_config().enable_eastmoney_patch:
            eastmoney_patch()
    
    def _set_random_user_agent(self) -> None:
        """
        settingsrandom User-Agent
        
        viamodify requests Session  headers implement
        thisiskey anti-scrapingstrategyone of
        """
        try:
            import akshare as ak
            # akshare internaluse requests，Ipluralviaenvironment variableordirectlysettingsfromimpact
            # actualabove/upper akshare possiblynotdirectlyexpose session，herevia fake_useragent act asassupplement
            random_ua = random.choice(USER_AGENTS)
            logger.debug(f"settings User-Agent: {random_ua[:50]}...")
        except Exception as e:
            logger.debug(f"settings User-Agent failed: {e}")
    
    def _enforce_rate_limit(self) -> None:
        """
        mandatoryexecution rateconstraint
        
        strategy：
        1. checkdistancelast timerequesttimeinterval
        2. ifintervalinsufficient，supplementsleeptime
        3. thenafteragainexecuterandom jitter sleep
        """
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            min_interval = self.sleep_min
            if elapsed < min_interval:
                additional_sleep = min_interval - elapsed
                logger.debug(f"supplementsleep {additional_sleep:.2f} seconds")
                time.sleep(additional_sleep)
        
        # executerandom jitter sleep
        self.random_sleep(self.sleep_min, self.sleep_max)
        self._last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),  # at mostretry3times
        wait=wait_exponential(multiplier=1, min=2, max=30),  # indexbackoff：2, 4, 8... max30seconds
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        from Akshare get rawdata
        
        based on codetypeautomaticselect API：
        - US stock：not supported，raiseabnormalby YfinanceFetcher processing（Issue #311）
        - HK stock：use ak.stock_hk_hist()
        - ETF fund：use ak.fund_etf_hist_em()
        - normal A stocks：use ak.stock_zh_a_hist()
        
        process：
        1. determinecodetype（US stock/HK stock/ETF/A-share）
        2. settingsrandom User-Agent
        3. execution rateconstraint（random sleep）
        4. calltoshould akshare API
        5. processingreturndata
        """
        # based on codetypechoose different fetchmethod
        if _is_us_code(stock_code):
            # US stock：akshare  stock_us_daily API/interfaceadjustedexistsalreadyknowissue（parametersee Issue #311）
            # exchangeby YfinanceFetcher processing，ensureadjustedpriceconsistent
            raise DataFetchError(
                f"AkshareFetcher not supportedUS stock {stock_code}，please use YfinanceFetcher getcorrectadjustedprice"
            )
        elif _is_hk_code(stock_code):
            return self._fetch_hk_data(stock_code, start_date, end_date)
        elif _is_etf_code(stock_code):
            return self._fetch_etf_data(stock_code, start_date, end_date)
        else:
            return self._fetch_stock_data(stock_code, start_date, end_date)
    
    def _fetch_stock_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        get normal A stock historicaldata

        strategy：
        1. try firstEastmoneyAPI/interface (ak.stock_zh_a_hist)
        2. failedaftertrySina FinanceAPI/interface (ak.stock_zh_a_daily)
        3. mostaftertryTencent FinanceAPI/interface (ak.stock_zh_a_hist_tx)
        """
        # trylist
        methods = [
            (self._fetch_stock_data_em, "Eastmoney"),
            (self._fetch_stock_data_sina, "Sina Finance"),
            (self._fetch_stock_data_tx, "Tencent Finance"),
        ]

        last_error = None

        for fetch_method, source_name in methods:
            try:
                logger.info(f"[datasource] tryuse {source_name} get {stock_code}...")
                df = fetch_method(stock_code, start_date, end_date)

                if df is not None and not df.empty:
                    logger.info(f"[datasource] {source_name} fetch successful")
                    return df
            except Exception as e:
                last_error = e
                logger.warning(f"[datasource] {source_name} fetch failed: {e}")
                # continuingtry next

        # allallfailed
        raise DataFetchError(f"Akshare allchannelfetch failed: {last_error}")

    def _fetch_stock_data_em(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        get normal A stock historicaldata (Eastmoney)
        Data sources:ak.stock_zh_a_hist()
        """
        import akshare as ak

        # anti-blocking strategy 1: random User-Agent
        self._set_random_user_agent()

        # anti-blocking strategy 2: mandatorysleep
        self._enforce_rate_limit()

        logger.info(f"[APIcall] ak.stock_zh_a_hist(symbol={stock_code}, ...)")

        try:
            import time as _time
            api_start = _time.time()

            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"
            )

            api_elapsed = _time.time() - api_start

            if df is not None and not df.empty:
                logger.info(f"[APIreturn] ak.stock_zh_a_hist successful: {len(df)} row, elapsed {api_elapsed:.2f}s")
                return df
            else:
                logger.warning(f"[APIreturn] ak.stock_zh_a_hist return emptydata")
                return pd.DataFrame()

        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['banned', 'blocked', 'frequency', 'rate', 'constraint']):
                raise RateLimitError(f"Akshare(EM) may berate limiting: {e}") from e
            raise e

    def _fetch_stock_data_sina(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        get normal A stock historicaldata (Sina Finance)
        Data sources:ak.stock_zh_a_daily()
        """
        import akshare as ak

        # convertingcodeformat：sh600000, sz000001, bj920748
        symbol = _to_sina_tx_symbol(stock_code)

        self._enforce_rate_limit()

        try:
            df = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"
            )

            # standardizeSinadatacolumn name
            # Sinareturn：date, open, high, low, close, volume, amount, outstanding_share, turnover
            if df is not None and not df.empty:
                # ensuredatecolumnexists
                if 'date' in df.columns:
                    df = df.rename(columns={'date': 'date'})

                # mappingothercolumnwithmatch _normalize_data expectation
                # _normalize_data expectation：date, open, close, highest, lowest, trading volume, trading amount
                rename_map = {
                    'open': 'open', 'high': 'highest', 'low': 'lowest',
                    'close': 'close', 'volume': 'trading volume', 'amount': 'trading amount'
                }
                df = df.rename(columns=rename_map)

                # calculatingprice change percentage（SinaAPI/interfacepossiblynotreturn）
                if 'close' in df.columns:
                    df['price change percentage'] = df['close'].pct_change() * 100
                    df['price change percentage'] = df['price change percentage'].fillna(0)

                return df
            return pd.DataFrame()

        except Exception as e:
            raise e

    def _fetch_stock_data_tx(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        get normal A stock historicaldata (Tencent Finance)
        Data sources:ak.stock_zh_a_hist_tx()
        """
        import akshare as ak

        # convertingcodeformat：sh600000, sz000001, bj920748
        symbol = _to_sina_tx_symbol(stock_code)

        self._enforce_rate_limit()

        try:
            df = ak.stock_zh_a_hist_tx(
                symbol=symbol,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"
            )

            # standardizeTencentdatacolumn name
            # Tencentreturn：date, open, close, high, low, volume, amount
            if df is not None and not df.empty:
                rename_map = {
                    'date': 'date', 'open': 'open', 'high': 'highest',
                    'low': 'lowest', 'close': 'close', 'volume': 'trading volume',
                    'amount': 'trading amount'
                }
                df = df.rename(columns=rename_map)

                # Tencentdatausuallypackageinclude 'price change percentage'，if nothencalculating
                if 'pct_chg' in df.columns:
                    df = df.rename(columns={'pct_chg': 'price change percentage'})
                elif 'close' in df.columns:
                    df['price change percentage'] = df['close'].pct_change() * 100
                    df['price change percentage'] = df['price change percentage'].fillna(0)

                return df
            return pd.DataFrame()

        except Exception as e:
            raise e
    
    def _fetch_etf_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        get ETF fundhistoricaldata
        
        Data sources:ak.fund_etf_hist_em()
        
        Args:
            stock_code: ETF code，e.g. '512400', '159883'
            start_date: startingdate，format 'YYYY-MM-DD'
            end_date: end date，format 'YYYY-MM-DD'
            
        Returns:
            ETF historicaldata DataFrame
        """
        import akshare as ak
        
        # anti-blocking strategy 1: random User-Agent
        self._set_random_user_agent()
        
        # anti-blocking strategy 2: mandatorysleep
        self._enforce_rate_limit()
        
        logger.info(f"[APIcall] ak.fund_etf_hist_em(symbol={stock_code}, period=daily, "
                   f"start_date={start_date.replace('-', '')}, end_date={end_date.replace('-', '')}, adjust=qfq)")
        
        try:
            import time as _time
            api_start = _time.time()
            
            # call akshare get ETF daily linedata
            df = ak.fund_etf_hist_em(
                symbol=stock_code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"  # forward adjusted
            )
            
            api_elapsed = _time.time() - api_start
            
            # recordreturndatasummary
            if df is not None and not df.empty:
                logger.info(f"[APIreturn] ak.fund_etf_hist_em successful: return {len(df)} rowdata, elapsed {api_elapsed:.2f}s")
                logger.info(f"[APIreturn] column name: {list(df.columns)}")
                logger.info(f"[APIreturn] daterange: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
                logger.debug(f"[APIreturn] latest3itemsdata:\n{df.tail(3).to_string()}")
            else:
                logger.warning(f"[APIreturn] ak.fund_etf_hist_em return emptydata, elapsed {api_elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # detect anti-scraping ban
            if any(keyword in error_msg for keyword in ['banned', 'blocked', 'frequency', 'rate', 'constraint']):
                logger.warning(f"detected possible ban: {e}")
                raise RateLimitError(f"Akshare may berate limiting: {e}") from e
            
            raise DataFetchError(f"Akshare get ETF datafailed: {e}") from e
    
    def _fetch_us_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        getUS stockhistoricaldata
        
        Data sources:ak.stock_us_daily()（Sina FinanceAPI/interface）
        
        Args:
            stock_code: US stockcode，e.g. 'AMD', 'AAPL', 'TSLA'
            start_date: startingdate，format 'YYYY-MM-DD'
            end_date: end date，format 'YYYY-MM-DD'
            
        Returns:
            US stockhistoricaldata DataFrame
        """
        import akshare as ak
        
        # anti-blocking strategy 1: random User-Agent
        self._set_random_user_agent()
        
        # anti-blocking strategy 2: mandatorysleep
        self._enforce_rate_limit()
        
        # US stockcodedirectlyuseuppercase
        symbol = stock_code.strip().upper()
        
        logger.info(f"[APIcall] ak.stock_us_daily(symbol={symbol}, adjust=qfq)")
        
        try:
            import time as _time
            api_start = _time.time()
            
            # call akshare getUS stockdaily linedata
            # stock_us_daily returnallhistoricaldata，aftercontinueneedby datefiltering
            df = ak.stock_us_daily(
                symbol=symbol,
                adjust="qfq"  # forward adjusted
            )
            
            api_elapsed = _time.time() - api_start
            
            # recordreturndatasummary
            if df is not None and not df.empty:
                logger.info(f"[APIreturn] ak.stock_us_daily successful: return {len(df)} rowdata, elapsed {api_elapsed:.2f}s")
                logger.info(f"[APIreturn] column name: {list(df.columns)}")
                
                # by datefiltering
                df['date'] = pd.to_datetime(df['date'])
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
                
                if not df.empty:
                    logger.info(f"[APIreturn] filteringafterdaterange: {df['date'].iloc[0].strftime('%Y-%m-%d')} ~ {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
                    logger.debug(f"[APIreturn] latest3itemsdata:\n{df.tail(3).to_string()}")
                else:
                    logger.warning(f"[APIreturn] filteringafterdata is empty，daterange {start_date} ~ {end_date} nodata")
                
                # convertingcolumn nameasChineseformatwithmatch _normalize_data
                # stock_us_daily return: date, open, high, low, close, volume
                rename_map = {
                    'date': 'date',
                    'open': 'open',
                    'high': 'highest',
                    'low': 'lowest',
                    'close': 'close',
                    'volume': 'trading volume',
                }
                df = df.rename(columns=rename_map)
                
                # calculatingprice change percentage（US stockAPI/interfacedo not return directly）
                if 'close' in df.columns:
                    df['price change percentage'] = df['close'].pct_change() * 100
                    df['price change percentage'] = df['price change percentage'].fillna(0)
                
                # estimatetrading amount（US stockAPI/interfacenotreturn）
                if 'trading volume' in df.columns and 'close' in df.columns:
                    df['trading amount'] = df['trading volume'] * df['close']
                else:
                    df['trading amount'] = 0
                
                return df
            else:
                logger.warning(f"[APIreturn] ak.stock_us_daily return emptydata, elapsed {api_elapsed:.2f}s")
                return pd.DataFrame()
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # detect anti-scraping ban
            if any(keyword in error_msg for keyword in ['banned', 'blocked', 'frequency', 'rate', 'constraint']):
                logger.warning(f"detected possible ban: {e}")
                raise RateLimitError(f"Akshare may berate limiting: {e}") from e
            
            raise DataFetchError(f"Akshare getUS stockdatafailed: {e}") from e

    def _fetch_hk_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        getHK stockhistoricaldata
        
        Data sources:ak.stock_hk_hist()
        
        Args:
            stock_code: HK stockcode，e.g. '00700', '01810'
            start_date: startingdate，format 'YYYY-MM-DD'
            end_date: end date，format 'YYYY-MM-DD'
            
        Returns:
            HK stockhistoricaldata DataFrame
        """
        import akshare as ak
        
        # anti-blocking strategy 1: random User-Agent
        self._set_random_user_agent()
        
        # anti-blocking strategy 2: mandatorysleep
        self._enforce_rate_limit()
        
        # ensurecodeformatcorrect（5digit number）
        code = stock_code.lower().replace('hk', '').zfill(5)
        
        logger.info(f"[APIcall] ak.stock_hk_hist(symbol={code}, period=daily, "
                   f"start_date={start_date.replace('-', '')}, end_date={end_date.replace('-', '')}, adjust=qfq)")
        
        try:
            import time as _time
            api_start = _time.time()
            
            # call akshare getHK stockdaily linedata
            df = ak.stock_hk_hist(
                symbol=code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"  # forward adjusted
            )
            
            api_elapsed = _time.time() - api_start
            
            # recordreturndatasummary
            if df is not None and not df.empty:
                logger.info(f"[APIreturn] ak.stock_hk_hist successful: return {len(df)} rowdata, elapsed {api_elapsed:.2f}s")
                logger.info(f"[APIreturn] column name: {list(df.columns)}")
                logger.info(f"[APIreturn] daterange: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
                logger.debug(f"[APIreturn] latest3itemsdata:\n{df.tail(3).to_string()}")
            else:
                logger.warning(f"[APIreturn] ak.stock_hk_hist return emptydata, elapsed {api_elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # detect anti-scraping ban
            if any(keyword in error_msg for keyword in ['banned', 'blocked', 'frequency', 'rate', 'constraint']):
                logger.warning(f"detected possible ban: {e}")
                raise RateLimitError(f"Akshare may berate limiting: {e}") from e
            
            raise DataFetchError(f"Akshare getHK stockdatafailed: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        standardize Akshare data
        
        Akshare returned column names（Chinese）：
        date, open, close, highest, lowest, trading volume, trading amount, amplitude, price change percentage, price change amount, turnover rate
        
        need to map to standard column names：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()
        
        # column name mapping（Akshare Chinesecolumn name -> standardEnglishcolumn name）
        column_mapping = {
            'date': 'date',
            'open': 'open',
            'close': 'close',
            'highest': 'high',
            'lowest': 'low',
            'trading volume': 'volume',
            'trading amount': 'amount',
            'price change percentage': 'pct_chg',
        }
        
        # renamenamecolumn
        df = df.rename(columns=column_mapping)
        
        # addstock codecolumn
        df['code'] = stock_code
        
        # keep only needed columns
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df
    
    def get_realtime_quote(self, stock_code: str, source: str = "em") -> Optional[UnifiedRealtimeQuote]:
        """
        get realtimequote/market datadata（support multipledatasource）

        datasource priority（canconfiguration）：
        1. em: Eastmoney（akshare ak.stock_zh_a_spot_em）- datamost complete，includevolume ratio/PE/PB/marketvalueetc
        2. sina: Sina Finance（akshare ak.stock_zh_a_spot）- lightweight，basicquote/market data
        3. tencent: Tencentdirectconnectingport - singlestockquerying，loadsmall

        Args:
            stock_code: stock/ETFcode
            source: datasourcetype，optional "em", "sina", "tencent"

        Returns:
            UnifiedRealtimeQuote object，fetch failedreturn None
        """
        circuit_breaker = get_realtime_circuit_breaker()

        # based on codetypechoose different fetchmethod
        if _is_us_code(stock_code):
            # US stocknotuse Akshare，by YfinanceFetcher processing
            logger.debug(f"[APIskip] {stock_code} isUS stock，Akshare not supportedUS stockrealtimequote/market data")
            return None
        elif _is_hk_code(stock_code):
            return self._get_hk_realtime_quote(stock_code)
        elif _is_etf_code(stock_code):
            source_key = "akshare_etf"
            if not circuit_breaker.is_available(source_key):
                logger.warning(f"[circuit break] datasource {source_key} atcircuit breakstatus，skip")
                return None
            return self._get_etf_realtime_quote(stock_code)
        else:
            source_key = f"akshare_{source}"
            if not circuit_breaker.is_available(source_key):
                logger.warning(f"[circuit break] datasource {source_key} atcircuit breakstatus，skip")
                return None
            # normal A stocks：based on source selectdatasource
            if source == "sina":
                return self._get_stock_realtime_quote_sina(stock_code)
            elif source == "tencent":
                return self._get_stock_realtime_quote_tencent(stock_code)
            else:
                return self._get_stock_realtime_quote_em(stock_code)
    
    def _get_stock_realtime_quote_em(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        get normal A stock realtimequote/market datadata（Eastmoneydatasource）
        
        Data sources:ak.stock_zh_a_spot_em()
        advantages：datamost complete，includevolume ratio、turnover rate、P/E ratio、P/B ratio、total market cap、circulating market capetc
        disadvantages：full pull，datalarge volume，easytimeout/rate limiting
        """
        import akshare as ak
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "akshare_em"
        
        try:
            # checkcache
            current_time = time.time()
            if (_realtime_cache['data'] is not None and 
                current_time - _realtime_cache['timestamp'] < _realtime_cache['ttl']):
                df = _realtime_cache['data']
                cache_age = int(current_time - _realtime_cache['timestamp'])
                logger.debug(f"[cachehit] A-sharerealtimequote/market data(Eastmoney) - cacheage {cache_age}s/{_realtime_cache['ttl']}s")
            else:
                # trigger fullrefresh
                logger.info(f"[cachenothit] trigger fullrefresh A-sharerealtimequote/market data(Eastmoney)")
                last_error: Optional[Exception] = None
                df = None
                for attempt in range(1, 3):
                    try:
                        # anti-blocking strategy
                        self._set_random_user_agent()
                        self._enforce_rate_limit()

                        logger.info(f"[APIcall] ak.stock_zh_a_spot_em() getA-sharerealtimequote/market data... (attempt {attempt}/2)")
                        import time as _time
                        api_start = _time.time()

                        df = ak.stock_zh_a_spot_em()

                        api_elapsed = _time.time() - api_start
                        logger.info(f"[APIreturn] ak.stock_zh_a_spot_em successful: return {len(df)} onlystock, elapsed {api_elapsed:.2f}s")
                        circuit_breaker.record_success(source_key)
                        break
                    except Exception as e:
                        last_error = e
                        logger.warning(f"[APIerror] ak.stock_zh_a_spot_em fetch failed (attempt {attempt}/2): {e}")
                        time.sleep(min(2 ** attempt, 5))

                # updatingcache：successfulcachedata；failedalsocacheemptydata，avoidsameroundtasktosameAPI/interfacerepeatedrequest
                if df is None:
                    logger.error(f"[APIerror] ak.stock_zh_a_spot_em finalfailed: {last_error}")
                    circuit_breaker.record_failure(source_key, str(last_error))
                    df = pd.DataFrame()
                _realtime_cache['data'] = df
                _realtime_cache['timestamp'] = current_time
                logger.info(f"[cacheupdating] A-sharerealtimequote/market data(Eastmoney) cachealreadyrefresh，TTL={_realtime_cache['ttl']}s")

            if df is None or df.empty:
                logger.warning(f"[realtimequote/market data] A-sharerealtimequote/market datadata is empty，skip {stock_code}")
                return None
            
            # find specifiedstock
            row = df[df['code'] == stock_code]
            if row.empty:
                logger.warning(f"[APIreturn] not foundstock {stock_code} realtimequote/market data")
                return None
            
            row = row.iloc[0]
            
            # use realtime_types.py unified inconvertingfunction
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=str(row.get('name', '')),
                source=RealtimeSource.AKSHARE_EM,
                price=safe_float(row.get('latest price')),
                change_pct=safe_float(row.get('price change percentage')),
                change_amount=safe_float(row.get('price change amount')),
                volume=safe_int(row.get('trading volume')),
                amount=safe_float(row.get('trading amount')),
                volume_ratio=safe_float(row.get('volume ratio')),
                turnover_rate=safe_float(row.get('turnover rate')),
                amplitude=safe_float(row.get('amplitude')),
                open_price=safe_float(row.get('today open')),
                high=safe_float(row.get('highest')),
                low=safe_float(row.get('lowest')),
                pe_ratio=safe_float(row.get('P/E ratio-dynamic')),
                pb_ratio=safe_float(row.get('P/B ratio')),
                total_mv=safe_float(row.get('total market cap')),
                circ_mv=safe_float(row.get('circulating market cap')),
                change_60d=safe_float(row.get('60dayprice change percentage')),
                high_52w=safe_float(row.get('52weekly high')),
                low_52w=safe_float(row.get('52weekly low')),
            )
            
            logger.info(f"[realtimequote/market data-Eastmoney] {stock_code} {quote.name}: price={quote.price}, price change={quote.change_pct}%, "
                       f"volume ratio={quote.volume_ratio}, turnover rate={quote.turnover_rate}%")
            return quote
            
        except Exception as e:
            logger.error(f"[APIerror] get {stock_code} realtimequote/market data(Eastmoney)failed: {e}")
            circuit_breaker.record_failure(source_key, str(e))
            return None
    
    def _get_stock_realtime_quote_sina(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        get normal A stock realtimequote/market datadata（Sina Financedatasource）
        
        Data sources:Sina FinanceAPI/interface（direct connect，singlestockquerying）
        advantages：singlestockquerying，loadsmall，speedfast
        disadvantages：datafieldfewer，novolume ratio/PE/PBetc
        
        API/interfaceformat：http://hq.sinajs.cn/list=sh600519,sz000001
        """
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "akshare_sina"
        symbol = _to_sina_tx_symbol(stock_code)
        url = f"http://{SINA_REALTIME_ENDPOINT}={symbol}"
        api_start = time.time()
        
        try:
            headers = {
                'Referer': 'http://finance.sina.com.cn',
                'User-Agent': random.choice(USER_AGENTS)
            }
            
            logger.info(
                f"[APIcall] Sina FinanceAPI/interfaceget {stock_code} realtimequote/market data: endpoint={SINA_REALTIME_ENDPOINT}, symbol={symbol}"
            )
            
            self._enforce_rate_limit()
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'gbk'
            api_elapsed = time.time() - api_start
            
            if response.status_code != 200:
                failure_message = _build_realtime_failure_message(
                    source_name="Sina",
                    endpoint=SINA_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="http_status",
                    detail=f"HTTP {response.status_code}",
                    elapsed=api_elapsed,
                    error_type="HTTPStatus",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            # parsingdata：var hq_str_sh600519="Kweichow Moutai,1866.000,1870.000,..."
            content = response.text.strip()
            if '=""' in content or not content:
                failure_message = _build_realtime_failure_message(
                    source_name="Sina",
                    endpoint=SINA_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="empty_response",
                    detail="empty quote payload",
                    elapsed=api_elapsed,
                    error_type="EmptyResponse",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            # extractleadwithin numberdata
            data_start = content.find('"')
            data_end = content.rfind('"')
            if data_start == -1 or data_end == -1:
                failure_message = _build_realtime_failure_message(
                    source_name="Sina",
                    endpoint=SINA_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="malformed_payload",
                    detail="quote payload missing quotes",
                    elapsed=api_elapsed,
                    error_type="MalformedPayload",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            data_str = content[data_start+1:data_end]
            fields = data_str.split(',')
            
            if len(fields) < 32:
                failure_message = _build_realtime_failure_message(
                    source_name="Sina",
                    endpoint=SINA_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="insufficient_fields",
                    detail=f"field_count={len(fields)}",
                    elapsed=api_elapsed,
                    error_type="InsufficientFields",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            circuit_breaker.record_success(source_key)
            
            # Sinadatafieldorder：
            # 0:name 1:today open 2:yesterday close 3:latest price 4:highest 5:lowest 6:buyoneprice 7:selloneprice
            # 8:trading volume(stocks) 9:trading amount(yuan) ... 30:date 31:time
            # use realtime_types.py unified inconvertingfunction
            price = safe_float(fields[3])
            pre_close = safe_float(fields[2])
            change_pct = None
            change_amount = None
            if price and pre_close and pre_close > 0:
                change_amount = price - pre_close
                change_pct = (change_amount / pre_close) * 100
            
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=fields[0],
                source=RealtimeSource.AKSHARE_SINA,
                price=price,
                change_pct=change_pct,
                change_amount=change_amount,
                volume=safe_int(fields[8]),  # trading volume（stocks）
                amount=safe_float(fields[9]),  # trading amount（yuan）
                open_price=safe_float(fields[1]),
                high=safe_float(fields[4]),
                low=safe_float(fields[5]),
                pre_close=pre_close,
            )
            
            logger.info(
                f"[realtimequote/market data-Sina] {stock_code} {quote.name}: endpoint={SINA_REALTIME_ENDPOINT}, "
                f"price={quote.price}, price change={quote.change_pct}, trading volume={quote.volume}, elapsed={api_elapsed:.2f}s"
            )
            return quote
            
        except Exception as e:
            api_elapsed = time.time() - api_start
            category, detail = _classify_realtime_http_error(e)
            failure_message = _build_realtime_failure_message(
                source_name="Sina",
                endpoint=SINA_REALTIME_ENDPOINT,
                stock_code=stock_code,
                symbol=symbol,
                category=category,
                detail=detail,
                elapsed=api_elapsed,
                error_type=type(e).__name__,
            )
            logger.error(failure_message)
            circuit_breaker.record_failure(source_key, failure_message)
            return None
    
    def _get_stock_realtime_quote_tencent(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        get normal A stock realtimequote/market datadata（Tencent Financedatasource）
        
        Data sources:Tencent FinanceAPI/interface（direct connect，singlestockquerying）
        advantages：singlestockquerying，loadsmall，packageincludeturnover rate
        disadvantages：novolume ratio/PE/PBetcestimatevaluedata
        
        API/interfaceformat：http://qt.gtimg.cn/q=sh600519,sz000001
        """
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "akshare_tencent"
        symbol = _to_sina_tx_symbol(stock_code)
        url = f"http://{TENCENT_REALTIME_ENDPOINT}={symbol}"
        api_start = time.time()
        
        try:
            headers = {
                'Referer': 'http://finance.qq.com',
                'User-Agent': random.choice(USER_AGENTS)
            }
            
            logger.info(
                f"[APIcall] Tencent FinanceAPI/interfaceget {stock_code} realtimequote/market data: endpoint={TENCENT_REALTIME_ENDPOINT}, symbol={symbol}"
            )
            
            self._enforce_rate_limit()
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'gbk'
            api_elapsed = time.time() - api_start
            
            if response.status_code != 200:
                failure_message = _build_realtime_failure_message(
                    source_name="Tencent",
                    endpoint=TENCENT_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="http_status",
                    detail=f"HTTP {response.status_code}",
                    elapsed=api_elapsed,
                    error_type="HTTPStatus",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            content = response.text.strip()
            if '=""' in content or not content:
                failure_message = _build_realtime_failure_message(
                    source_name="Tencent",
                    endpoint=TENCENT_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="empty_response",
                    detail="empty quote payload",
                    elapsed=api_elapsed,
                    error_type="EmptyResponse",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            # extractdata
            data_start = content.find('"')
            data_end = content.rfind('"')
            if data_start == -1 or data_end == -1:
                failure_message = _build_realtime_failure_message(
                    source_name="Tencent",
                    endpoint=TENCENT_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="malformed_payload",
                    detail="quote payload missing quotes",
                    elapsed=api_elapsed,
                    error_type="MalformedPayload",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            data_str = content[data_start+1:data_end]
            fields = data_str.split('~')

            if len(fields) < 45:
                failure_message = _build_realtime_failure_message(
                    source_name="Tencent",
                    endpoint=TENCENT_REALTIME_ENDPOINT,
                    stock_code=stock_code,
                    symbol=symbol,
                    category="insufficient_fields",
                    detail=f"field_count={len(fields)}",
                    elapsed=api_elapsed,
                    error_type="InsufficientFields",
                )
                logger.warning(failure_message)
                circuit_breaker.record_failure(source_key, failure_message)
                return None
            
            circuit_breaker.record_success(source_key)
            
            # Tencentdatafieldorder（complete）：
            # 1:name 2:code 3:latest price 4:yesterday close 5:today open 6:trading volume(hand) 7:outsideplate 8:inplate
            # 9-28:buy/sellfivelevel 30:timestamp 31:price change amount 32:price change percentage(%) 33:highest 34:lowest 35:close/trading volume/trading amount
            # 36:trading volume(hand) 37:trading amount(ten thousand) 38:turnover rate(%) 39:P/E ratio 43:amplitude(%)
            # 44:circulating market cap(hundred million) 45:total market cap(hundred million) 46:P/B ratio 47:limit upprice 48:limit downprice 49:volume ratio
            # use realtime_types.py unified inconvertingfunction
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=fields[1] if len(fields) > 1 else "",
                source=RealtimeSource.TENCENT,
                price=safe_float(fields[3]),
                change_pct=safe_float(fields[32]),
                change_amount=safe_float(fields[31]) if len(fields) > 31 else None,
                volume=safe_int(fields[6]) * 100 if fields[6] else None,  # Tencentreturnedishand，convertasstocks
                open_price=safe_float(fields[5]),
                high=safe_float(fields[33]) if len(fields) > 33 else None,  # correct：field 33 ishighest price
                low=safe_float(fields[34]) if len(fields) > 34 else None,  # correct：field 34 islowest price
                pre_close=safe_float(fields[4]),
                turnover_rate=safe_float(fields[38]) if len(fields) > 38 else None,
                amplitude=safe_float(fields[43]) if len(fields) > 43 else None,
                volume_ratio=safe_float(fields[49]) if len(fields) > 49 else None,  # volume ratio
                pe_ratio=safe_float(fields[39]) if len(fields) > 39 else None,  # P/E ratio
                pb_ratio=safe_float(fields[46]) if len(fields) > 46 else None,  # P/B ratio
                circ_mv=safe_float(fields[44]) * 100000000 if len(fields) > 44 and fields[44] else None,  # circulating market cap(hundred million->yuan)
                total_mv=safe_float(fields[45]) * 100000000 if len(fields) > 45 and fields[45] else None,  # total market cap(hundred million->yuan)
            )
            
            logger.info(
                f"[realtimequote/market data-Tencent] {stock_code} {quote.name}: endpoint={TENCENT_REALTIME_ENDPOINT}, "
                f"price={quote.price}, price change={quote.change_pct}%, volume ratio={quote.volume_ratio}, "
                f"turnover rate={quote.turnover_rate}%, elapsed={api_elapsed:.2f}s"
            )
            return quote
            
        except Exception as e:
            api_elapsed = time.time() - api_start
            category, detail = _classify_realtime_http_error(e)
            failure_message = _build_realtime_failure_message(
                source_name="Tencent",
                endpoint=TENCENT_REALTIME_ENDPOINT,
                stock_code=stock_code,
                symbol=symbol,
                category=category,
                detail=detail,
                elapsed=api_elapsed,
                error_type=type(e).__name__,
            )
            logger.error(failure_message)
            circuit_breaker.record_failure(source_key, failure_message)
            return None
    
    def _get_etf_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        get ETF fundrealtimequote/market datadata
        
        Data sources:ak.fund_etf_spot_em()
        packageinclude：latest price、price change percentage、trading volume、trading amount、turnover rateetc
        
        Args:
            stock_code: ETF code
            
        Returns:
            UnifiedRealtimeQuote object，fetch failedreturn None
        """
        import akshare as ak
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "akshare_etf"
        
        try:
            # checkcache
            current_time = time.time()
            if (_etf_realtime_cache['data'] is not None and 
                current_time - _etf_realtime_cache['timestamp'] < _etf_realtime_cache['ttl']):
                df = _etf_realtime_cache['data']
                logger.debug(f"[cachehit] usecacheETFrealtimequote/market datadata")
            else:
                last_error: Optional[Exception] = None
                df = None
                for attempt in range(1, 3):
                    try:
                        # anti-blocking strategy
                        self._set_random_user_agent()
                        self._enforce_rate_limit()

                        logger.info(f"[APIcall] ak.fund_etf_spot_em() getETFrealtimequote/market data... (attempt {attempt}/2)")
                        import time as _time
                        api_start = _time.time()

                        df = ak.fund_etf_spot_em()

                        api_elapsed = _time.time() - api_start
                        logger.info(f"[APIreturn] ak.fund_etf_spot_em successful: return {len(df)} onlyETF, elapsed {api_elapsed:.2f}s")
                        circuit_breaker.record_success(source_key)
                        break
                    except Exception as e:
                        last_error = e
                        logger.warning(f"[APIerror] ak.fund_etf_spot_em fetch failed (attempt {attempt}/2): {e}")
                        time.sleep(min(2 ** attempt, 5))

                if df is None:
                    logger.error(f"[APIerror] ak.fund_etf_spot_em finalfailed: {last_error}")
                    circuit_breaker.record_failure(source_key, str(last_error))
                    df = pd.DataFrame()
                _etf_realtime_cache['data'] = df
                _etf_realtime_cache['timestamp'] = current_time

            if df is None or df.empty:
                logger.warning(f"[realtimequote/market data] ETFrealtimequote/market datadata is empty，skip {stock_code}")
                return None
            
            # find specified ETF
            row = df[df['code'] == stock_code]
            if row.empty:
                logger.warning(f"[APIreturn] not found ETF {stock_code} realtimequote/market data")
                return None
            
            row = row.iloc[0]
            
            # use realtime_types.py unified inconvertingfunction
            # ETF quote/market datadatabuild
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=str(row.get('name', '')),
                source=RealtimeSource.AKSHARE_EM,
                price=safe_float(row.get('latest price')),
                change_pct=safe_float(row.get('price change percentage')),
                change_amount=safe_float(row.get('price change amount')),
                volume=safe_int(row.get('trading volume')),
                amount=safe_float(row.get('trading amount')),
                volume_ratio=safe_float(row.get('volume ratio')),
                turnover_rate=safe_float(row.get('turnover rate')),
                amplitude=safe_float(row.get('amplitude')),
                open_price=safe_float(row.get('opening price')),
                high=safe_float(row.get('highest price')),
                low=safe_float(row.get('lowest price')),
                total_mv=safe_float(row.get('total market cap')),
                circ_mv=safe_float(row.get('circulating market cap')),
                high_52w=safe_float(row.get('52weekly high')),
                low_52w=safe_float(row.get('52weekly low')),
            )
            
            logger.info(f"[ETFrealtimequote/market data] {stock_code} {quote.name}: price={quote.price}, price change={quote.change_pct}%, "
                       f"turnover rate={quote.turnover_rate}%")
            return quote
            
        except Exception as e:
            logger.error(f"[APIerror] get ETF {stock_code} realtimequote/market datafailed: {e}")
            circuit_breaker.record_failure(source_key, str(e))
            return None
    
    def _get_hk_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        getHK stockrealtimequote/market datadata
        
        Data sources:ak.stock_hk_spot_em()
        packageinclude：latest price、price change percentage、trading volume、trading amountetc
        
        Args:
            stock_code: HK stockcode
            
        Returns:
            UnifiedRealtimeQuote object，fetch failedreturn None
        """
        import akshare as ak
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "akshare_hk"

        if not circuit_breaker.is_available(source_key):
            logger.warning(f"[circuit break] datasource {source_key} atcircuit breakstatus，skip")
            return None
        
        try:
            # anti-blocking strategy
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            # ensurecodeformatcorrect（5digit number）
            raw_code = stock_code.strip().lower()
            if raw_code.endswith('.hk'):
                raw_code = raw_code[:-3]
            if raw_code.startswith('hk'):
                raw_code = raw_code[2:]
            code = raw_code.zfill(5)
            
            logger.info(f"[APIcall] ak.stock_hk_spot_em() getHK stockrealtimequote/market data...")
            import time as _time
            api_start = _time.time()
            
            df = ak.stock_hk_spot_em()
            
            api_elapsed = _time.time() - api_start
            logger.info(f"[APIreturn] ak.stock_hk_spot_em successful: return {len(df)} onlyHK stock, elapsed {api_elapsed:.2f}s")
            circuit_breaker.record_success(source_key)
            
            # find specifiedHK stock
            row = df[df['code'] == code]
            if row.empty:
                logger.warning(f"[APIreturn] not foundHK stock {code} realtimequote/market data")
                return None
            
            row = row.iloc[0]
            
            # use realtime_types.py unified inconvertingfunction
            # HK stockquote/market datadatabuild
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=str(row.get('name', '')),
                source=RealtimeSource.AKSHARE_EM,
                price=safe_float(row.get('latest price')),
                change_pct=safe_float(row.get('price change percentage')),
                change_amount=safe_float(row.get('price change amount')),
                volume=safe_int(row.get('trading volume')),
                amount=safe_float(row.get('trading amount')),
                volume_ratio=safe_float(row.get('volume ratio')),
                turnover_rate=safe_float(row.get('turnover rate')),
                amplitude=safe_float(row.get('amplitude')),
                pe_ratio=safe_float(row.get('P/E ratio')),
                pb_ratio=safe_float(row.get('P/B ratio')),
                total_mv=safe_float(row.get('total market cap')),
                circ_mv=safe_float(row.get('circulating market cap')),
                high_52w=safe_float(row.get('52weekly high')),
                low_52w=safe_float(row.get('52weekly low')),
            )
            
            logger.info(f"[HK stockrealtimequote/market data] {stock_code} {quote.name}: price={quote.price}, price change={quote.change_pct}%, "
                       f"turnover rate={quote.turnover_rate}%")
            return quote
            
        except Exception as e:
            logger.error(f"[APIerror] getHK stock {stock_code} realtimequote/market datafailed: {e}")
            circuit_breaker.record_failure(source_key, str(e))
            return None
    
    def get_chip_distribution(self, stock_code: str) -> Optional[ChipDistribution]:
        """
        get chip distributiondata
        
        Data sources:ak.stock_cyq_em()
        packageinclude：profitproportion、average cost、chip concentration
        
        Note：ETF/indexno chip distributiondata，willreturn directly None
        
        Args:
            stock_code: stock code
            
        Returns:
            ChipDistribution object（latestonedaysdata），fetch failedreturn None
        """
        import akshare as ak

        # US stockno chip distributiondata（Akshare not supported）
        if _is_us_code(stock_code):
            logger.debug(f"[APIskip] {stock_code} isUS stock，no chip distributiondata")
            return None

        # HK stockno chip distributiondata（stock_cyq_em is A stocksspecializedbelongAPI/interface）
        if _is_hk_code(stock_code):
            logger.debug(f"[APIskip] {stock_code} isHK stock，no chip distributiondata")
            return None

        # ETF/indexno chip distributiondata
        if _is_etf_code(stock_code):
            logger.debug(f"[APIskip] {stock_code} is ETF/index，no chip distributiondata")
            return None
        
        try:
            # anti-blocking strategy
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            logger.info(f"[APIcall] ak.stock_cyq_em(symbol={stock_code}) get chip distribution...")
            import time as _time
            api_start = _time.time()
            
            df = ak.stock_cyq_em(symbol=stock_code)
            
            api_elapsed = _time.time() - api_start
            
            if df.empty:
                logger.warning(f"[APIreturn] ak.stock_cyq_em return emptydata, elapsed {api_elapsed:.2f}s")
                return None
            
            logger.info(f"[APIreturn] ak.stock_cyq_em successful: return {len(df)} daysdata, elapsed {api_elapsed:.2f}s")
            logger.debug(f"[APIreturn] chipdatacolumn name: {list(df.columns)}")
            
            # getlatestonedaysdata
            latest = df.iloc[-1]
            
            # use realtime_types.py unified inconvertingfunction
            chip = ChipDistribution(
                code=stock_code,
                date=str(latest.get('date', '')),
                profit_ratio=safe_float(latest.get('profitproportion')),
                avg_cost=safe_float(latest.get('average cost')),
                cost_90_low=safe_float(latest.get('90cost-low')),
                cost_90_high=safe_float(latest.get('90cost-high')),
                concentration_90=safe_float(latest.get('90concentration')),
                cost_70_low=safe_float(latest.get('70cost-low')),
                cost_70_high=safe_float(latest.get('70cost-high')),
                concentration_70=safe_float(latest.get('70concentration')),
            )
            
            logger.info(f"[chip distribution] {stock_code} date={chip.date}: profitproportion={chip.profit_ratio:.1%}, "
                       f"average cost={chip.avg_cost}, 90%concentration={chip.concentration_90:.2%}, "
                       f"70%concentration={chip.concentration_70:.2%}")
            return chip
            
        except Exception as e:
            logger.error(f"[APIerror] get {stock_code} chip distributionfailed: {e}")
            return None
    
    def get_enhanced_data(self, stock_code: str, days: int = 60) -> Dict[str, Any]:
        """
        get enhanceddata（historicalcandlestick + realtimequote/market data + chip distribution）
        
        Args:
            stock_code: stock code
            days: historicaldatadayscount
            
        Returns:
            packageincluding alldatadictionary
        """
        result = {
            'code': stock_code,
            'daily_data': None,
            'realtime_quote': None,
            'chip_distribution': None,
        }
        
        # get daily datadata
        try:
            df = self.get_daily_data(stock_code, days=days)
            result['daily_data'] = df
        except Exception as e:
            logger.error(f"get {stock_code} daily linedatafailed: {e}")
        
        # get realtimequote/market data
        result['realtime_quote'] = self.get_realtime_quote(stock_code)
        
        # get chip distribution
        result['chip_distribution'] = self.get_chip_distribution(stock_code)
        
        return result

    def get_main_indices(self, region: str = "cn") -> Optional[List[Dict[str, Any]]]:
        """
        get mainindexrealtimequote/market data (SinaAPI/interface)，only support A stocks
        """
        if region != "cn":
            return None
        import akshare as ak

        # mainindexcodemapping
        indices_map = {
            'sh000001': 'Shanghai Compositeindex',
            'sz399001': 'Shenzhen Component Index',
            'sz399006': 'ChiNextrefers to',
            'sh000688': 'STAR50',
            'sh000016': 'Shanghai Composite50',
            'sh000300': 'Shanghai-Shenzhen300',
        }

        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            # use akshare getindexquote/market data（Sina FinanceAPI/interface）
            df = ak.stock_zh_index_spot_sina()

            results = []
            if df is not None and not df.empty:
                for code, name in indices_map.items():
                    # findtoshouldindex
                    row = df[df['code'] == code]
                    if row.empty:
                        # trywithprefixfind
                        row = df[df['code'].str.contains(code)]

                    if not row.empty:
                        row = row.iloc[0]
                        current = safe_float(row.get('latest price', 0))
                        prev_close = safe_float(row.get('yesterday close', 0))
                        high = safe_float(row.get('highest', 0))
                        low = safe_float(row.get('lowest', 0))

                        # calculatingamplitude
                        amplitude = 0.0
                        if prev_close > 0:
                            amplitude = (high - low) / prev_close * 100

                        results.append({
                            'code': code,
                            'name': name,
                            'current': current,
                            'change': safe_float(row.get('price change amount', 0)),
                            'change_pct': safe_float(row.get('price change percentage', 0)),
                            'open': safe_float(row.get('today open', 0)),
                            'high': high,
                            'low': low,
                            'prev_close': prev_close,
                            'volume': safe_float(row.get('trading volume', 0)),
                            'amount': safe_float(row.get('trading amount', 0)),
                            'amplitude': amplitude,
                        })
            return results

        except Exception as e:
            logger.error(f"[Akshare] getindexquote/market datafailed: {e}")
            return None

    def get_market_stats(self) -> Optional[Dict[str, Any]]:
        """
        get market gain/loss statistics

        datasource priority：
        1. EastmoneyAPI/interface (ak.stock_zh_a_spot_em)
        2. SinaAPI/interface (ak.stock_zh_a_spot)
        """
        import akshare as ak

        # priorityEastmoneyAPI/interface
        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info("[APIcall] ak.stock_zh_a_spot_em() get market statistics...")
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                return self._calc_market_stats(df)
        except Exception as e:
            logger.warning(f"[Akshare] EastmoneyAPI/interfaceget market statisticsfailed: {e}，try SinaAPI/interface")

        # Eastmoneyfailedafter，try SinaAPI/interface
        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info("[APIcall] ak.stock_zh_a_spot() get market statistics(Sina)...")
            df = ak.stock_zh_a_spot()
            if df is not None and not df.empty:
                return self._calc_market_stats(df)
        except Exception as e:
            logger.error(f"[Akshare] SinaAPI/interfaceget market statisticsalsofailed: {e}")

        return None

    def _calc_market_stats(
        self,
        df: pd.DataFrame,
        ) -> Optional[Dict[str, Any]]:
        """fromquote/market data DataFrame calculatinggain/loss statistics。"""
        import numpy as np

        df = df.copy()
        
        # 1. extractbasiccomparedata：latest price、yesterday close
        # compatible with differentAPI/interfacereturned column names sina/em efinance tushare xtdata
        code_col = next((c for c in ['code', 'stock code', 'ts_code','stock_code'] if c in df.columns), None)
        name_col = next((c for c in ['name', 'stockname','name','name'] if c in df.columns), None)
        close_col = next((c for c in ['latest price', 'latest price', 'close','lastPrice'] if c in df.columns), None)
        pre_close_col = next((c for c in ['yesterday close', 'prev close', 'pre_close','lastClose'] if c in df.columns), None)
        amount_col = next((c for c in ['trading amount', 'trading amount', 'amount','amount'] if c in df.columns), None) 
        
        limit_up_count = 0
        limit_down_count = 0
        up_count = 0
        down_count = 0
        flat_count = 0

        for code, name, current_price, pre_close, amount in zip(
            df[code_col], df[name_col], df[close_col], df[pre_close_col], df[amount_col]
        ):
            
            # trading suspendedfiltering efinance trading suspendeddatasometimes missing price shown as '-'，em shown asnone
            if pd.isna(current_price) or pd.isna(pre_close) or current_price in ['-'] or pre_close in ['-'] or amount == 0:
                continue
            
            # em、efinance asstr needconvertingasfloat
            current_price = float(current_price)
            pre_close = float(pre_close)
            
            # get numeric code without prefix
            pure_code = normalize_stock_code(str(code)) 

            # A. determine eachstockprice change percentageproportion (use numeric code to determine)
            if is_bse_code(pure_code): 
                ratio = 0.30
            elif is_kc_cy_stock(pure_code): #pure_code.startswith(('688', '30')):
                ratio = 0.20
            elif is_st_stock(name): #'ST' in str_name:
                ratio = 0.05
            else:
                ratio = 0.10

            # B. strictaccording to A stock rulescalculatinglimit up/down price：yesterday close * (1 ± proportion) -> round to2decimal places
            limit_up_price = np.floor(pre_close * (1 + ratio) * 100 + 0.5) / 100.0
            limit_down_price = np.floor(pre_close * (1 - ratio) * 100 + 0.5) / 100.0

            limit_up_price_Tolerance = round(abs(pre_close * (1 + ratio) - limit_up_price), 10)
            limit_down_price_Tolerance = round(abs(pre_close * (1 - ratio) - limit_down_price), 10)

            # C. exactcompare
            if current_price > 0 :
                is_limit_up = (current_price > 0) and (abs(current_price - limit_up_price) <= limit_up_price_Tolerance)
                is_limit_down = (current_price > 0) and (abs(current_price - limit_down_price) <= limit_down_price_Tolerance)

                if is_limit_up:
                    limit_up_count += 1
                if is_limit_down:
                    limit_down_count += 1

                if current_price > pre_close:
                    up_count += 1
                elif current_price < pre_close:
                    down_count += 1
                else:
                    flat_count += 1
                
        # count
        stats = {
            'up_count': up_count,
            'down_count': down_count,
            'flat_count': flat_count,
            'limit_up_count': limit_up_count,
            'limit_down_count': limit_down_count,
            'total_amount': 0.0,
        }
        
        # trading amountstatistics
        if amount_col and amount_col in df.columns:
            df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
            stats['total_amount'] = (df[amount_col].sum() / 1e8)
            
        return stats

    def get_sector_rankings(self, n: int = 5) -> Optional[Tuple[List[Dict], List[Dict]]]:
        """
        get industrysectorgain/loss rankings

        datasource priority：
        1. EastmoneyAPI/interface (ak.stock_board_industry_name_em)
        2. SinaAPI/interface (ak.stock_sector_spot)
        """
        import akshare as ak

        def _get_rank_top_n(df: pd.DataFrame, change_col: str, industry_name: str, n: int) -> Tuple[list, list]:
            df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
            df = df.dropna(subset=[change_col])

            # gain percentagebeforen
            top = df.nlargest(n, change_col)
            top_sectors = [
                {'name': row[industry_name], 'change_pct': row[change_col]}
                for _, row in top.iterrows()
            ]

            bottom = df.nsmallest(n, change_col)
            bottom_sectors = [
                {'name': row[industry_name], 'change_pct': row[change_col]}
                for _, row in bottom.iterrows()
            ]
            return top_sectors, bottom_sectors
        
        # priorityEastmoneyAPI/interface
        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info("[APIcall] ak.stock_board_industry_name_em() getsectorranking...")
            df = ak.stock_board_industry_name_em()
            if df is not None and not df.empty:
                change_col = 'price change percentage'
                name = 'sectorname'
                return _get_rank_top_n(df, change_col, name, n)
            
        except Exception as e:
            logger.warning(f"[Akshare] EastmoneyAPI/interfaceget industrysectorrankingfailed: {e}，try SinaAPI/interface")

        # Eastmoneyfailedafter，try SinaAPI/interface
        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info("[APIcall] ak.stock_sector_spot() get industrysectorranking(Sina)...")
            df = ak.stock_sector_spot(indicator='industry')
            if df is None or df.empty:
                return None
            change_col = 'price change percentage'
            name = 'sector'
            return _get_rank_top_n(df, change_col, name, n)
        
        except Exception as e:
            logger.error(f"[Akshare] SinaAPI/interfacegetsectorrankingalsofailed: {e}")
            return None


if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = AkshareFetcher()
    
    # testingnormalstock
    print("=" * 50)
    print("testingnormalstockdataget")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('600519')  # Maotai
        print(f"[stock] fetch successful，total {len(df)} itemsdata")
        print(df.tail())
    except Exception as e:
        print(f"[stock] fetch failed: {e}")
    
    # testing ETF fund
    print("\n" + "=" * 50)
    print("testing ETF funddataget")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('512400')  # hascolorleading stockETF
        print(f"[ETF] fetch successful，total {len(df)} itemsdata")
        print(df.tail())
    except Exception as e:
        print(f"[ETF] fetch failed: {e}")
    
    # testing ETF realtimequote/market data
    print("\n" + "=" * 50)
    print("testing ETF realtimequote/market dataget")
    print("=" * 50)
    try:
        quote = fetcher.get_realtime_quote('512880')  # securitiesETF
        if quote:
            print(f"[ETFrealtime] {quote.name}: price={quote.price}, price change percentage={quote.change_pct}%")
        else:
            print("[ETFrealtime] failed to getdata")
    except Exception as e:
        print(f"[ETFrealtime] fetch failed: {e}")
    
    # testingHK stockhistoricaldata
    print("\n" + "=" * 50)
    print("testingHK stockhistoricaldataget")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('00700')  # Tencent Holdings
        print(f"[HK stock] fetch successful，total {len(df)} itemsdata")
        print(df.tail())
    except Exception as e:
        print(f"[HK stock] fetch failed: {e}")
    
    # testingHK stockrealtimequote/market data
    print("\n" + "=" * 50)
    print("testingHK stockrealtimequote/market dataget")
    print("=" * 50)
    try:
        quote = fetcher.get_realtime_quote('00700')  # Tencent Holdings
        if quote:
            print(f"[HK stockrealtime] {quote.name}: price={quote.price}, price change percentage={quote.change_pct}%")
        else:
            print("[HK stockrealtime] failed to getdata")
    except Exception as e:
        print(f"[HK stockrealtime] fetch failed: {e}")

    # testingmarket statistics
    print("\n" + "=" * 50)
    print("Testing get_market_stats (akshare)")
    print("=" * 50)
    try:
        stats = fetcher.get_market_stats()
        if stats:
            print(f"Market Stats successfully computed:")
            print(f"Up: {stats['up_count']} (Limit Up: {stats['limit_up_count']})")
            print(f"Down: {stats['down_count']} (Limit Down: {stats['limit_down_count']})")
            print(f"Flat: {stats['flat_count']}")
            print(f"Total Amount: {stats['total_amount']:.2f} hundred million (Yi)")
        else:
            print("Failed to compute market stats.")
    except Exception as e:
        print(f"Failed to compute market stats: {e}")

    # testingchip distributiondata
    print("\n" + "=" * 50)
    print("testingchip distributiondataget")
    print("=" * 50)
    try:
        chip = fetcher.get_chip_distribution('600519')  # Maotai
    except Exception as e:
        print(f"[chip distribution] fetch failed: {e}")

    # testingindustrysectorranking
    print("\n" + "=" * 50)
    print("testingindustrysectorrankingget")
    print("=" * 50)
    try:
        rankings = fetcher.get_sector_rankings(n=5)
        if rankings:
            top, bottom = rankings
            print("gain rankings Top 5:")
            for sector in top:
                print(f"{sector['name']}: {sector['change_pct']}%")
            print("\nloss rankings Top 5:")
            for sector in bottom:
                print(f"{sector['name']}: {sector['change_pct']}%")
        else:
            print("failed to getindustrysectorrankingdata")
    except Exception as e:
        print(f"[industrysectorranking] fetch failed: {e}")
