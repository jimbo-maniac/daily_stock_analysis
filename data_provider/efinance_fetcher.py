# -*- coding: utf-8 -*-
"""
===================================
EfinanceFetcher - prioritydatasource (Priority 0)
===================================

Data sources:Eastmoneycrawler（via efinance library）
Features: free, no token required, comprehensive data、API concise
positionlibrary：https://github.com/Micro-sheep/efinance

with AkshareFetcher classsimilar，but efinance library：
1. API moreconciseeasyuse
2. supportbatch fetchdata
3. morestableAPI/interfaceencapsulation

Anti-blocking strategy:
1. each timerequestrandom sleep before 1.5-3.0 seconds
2. Randomly rotate User-Agent
3. Use tenacity for exponential backoff retry
4. Circuit breaker mechanism: automatic cooldown after consecutive failures
"""

import logging
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests  # leadenter requests withcaptureabnormal
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Timeout (seconds) for efinance library calls that go through eastmoney APIs
# with no built-in timeout.  Prevents indefinite hangs when hosts are unreachable.
try:
    _EF_CALL_TIMEOUT = int(os.environ.get("EFINANCE_CALL_TIMEOUT", "30"))
except (ValueError, TypeError):
    import logging as _logging
    _logging.getLogger(__name__).warning(
        "EFINANCE_CALL_TIMEOUT is not a valid integer, using default 30s"
    )
    _EF_CALL_TIMEOUT = 30

from patch.eastmoney_patch import eastmoney_patch
from src.config import get_config
from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS,is_bse_code, is_st_stock, is_kc_cy_stock, normalize_stock_code
from .realtime_types import (
    UnifiedRealtimeQuote, RealtimeSource,
    get_realtime_circuit_breaker,
    safe_float, safe_int  # Use unified type conversion functions
)


# keepoldtypealias，fortoaftercompatible
@dataclass
class EfinanceRealtimeQuote:
    """
    realtimequote/market datadata（from efinance）- toaftercompatiblealias
    
    newcoderecommendeduse UnifiedRealtimeQuote
    """
    code: str
    name: str = ""
    price: float = 0.0           # latest price
    change_pct: float = 0.0      # price change percentage(%)
    change_amount: float = 0.0   # price change amount
    
    # volume-priceindicator
    volume: int = 0              # trading volume
    amount: float = 0.0          # trading amount
    turnover_rate: float = 0.0   # turnover rate(%)
    amplitude: float = 0.0       # amplitude(%)
    
    # priceinterval
    high: float = 0.0            # highest price
    low: float = 0.0             # lowest price
    open_price: float = 0.0      # opening price
    
    def to_dict(self) -> Dict[str, Any]:
        """convertingasdictionary"""
        return {
            'code': self.code,
            'name': self.name,
            'price': self.price,
            'change_pct': self.change_pct,
            'change_amount': self.change_amount,
            'volume': self.volume,
            'amount': self.amount,
            'turnover_rate': self.turnover_rate,
            'amplitude': self.amplitude,
            'high': self.high,
            'low': self.low,
            'open': self.open_price,
        }


logger = logging.getLogger(__name__)

EASTMONEY_HISTORY_ENDPOINT = "push2his.eastmoney.com/api/qt/stock/kline/get"


# User-Agent pool for random rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]


# Cache real-time quote data (avoid duplicate requests)
# TTL set to 10 minutes (600seconds)：batchanalyzingscenariobelowavoid duplicatepull
_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 600  # 10minutescachevalidity period
}

# ETF real-time quote cache（withstockminuteopencache）
_etf_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 600  # 10minutescachevalidity period
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
    return stock_code.startswith(etf_prefixes) and len(stock_code) == 6


def _is_us_code(stock_code: str) -> bool:
    """
    check if code isUS stock
    
    US stockcode rules：
    - 1-5uppercase letters，e.g. 'AAPL', 'TSLA'
    - possiblypackageinclude '.'，e.g. 'BRK.B'
    """
    code = stock_code.strip().upper()
    return bool(re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', code))


def _ef_call_with_timeout(func, *args, timeout=None, **kwargs):
    """Run an efinance library call in a thread with a timeout.

    efinance internally uses requests/urllib3 with no timeout, so when
    eastmoney hosts are unreachable the call can hang for many minutes.
    This helper caps the *calling thread's* wait time.  Note: Python threads
    cannot be forcibly killed, so the worker thread may continue running in
    the background until the OS-level TCP timeout fires or the process exits.
    This is acceptable — the calling thread returns promptly on timeout.
    """
    if timeout is None:
        timeout = _EF_CALL_TIMEOUT
    # Do NOT use 'with ThreadPoolExecutor(...)' here: the context manager calls
    # shutdown(wait=True) on __exit__, which would re-block on the hung thread.
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(func, *args, **kwargs)
        return future.result(timeout=timeout)
    finally:
        # wait=False: calling thread returns immediately; worker cleans up later
        executor.shutdown(wait=False)


def _classify_eastmoney_error(exc: Exception) -> Tuple[str, str]:
    """
    Classify Eastmoney request failures into stable log categories.
    """
    message = str(exc).strip()
    lowered = message.lower()

    remote_disconnect_keywords = (
        'remotedisconnected',
        'remote end closed connection without response',
        'connection aborted',
        'connection broken',
        'protocolerror',
    )
    timeout_keywords = (
        'timeout',
        'timed out',
        'readtimeout',
        'connecttimeout',
    )
    rate_limit_keywords = (
        'banned',
        'blocked',
        'frequency',
        'rate limit',
        'too many requests',
        '429',
        'constraint',
        'forbidden',
        '403',
    )

    if any(keyword in lowered for keyword in remote_disconnect_keywords):
        return "remote_disconnect", message
    if isinstance(exc, (TimeoutError, requests.exceptions.Timeout)) or any(
        keyword in lowered for keyword in timeout_keywords
    ):
        return "timeout", message
    if any(keyword in lowered for keyword in rate_limit_keywords):
        return "rate_limit_or_anti_bot", message
    if isinstance(exc, requests.exceptions.RequestException):
        return "request_error", message
    return "unknown_request_error", message


class EfinanceFetcher(BaseFetcher):
    """
    Efinance datasource implementation
    
    priority：0（highest，priorityat AkshareFetcher）
    Data sources:Eastmoneynetwork（via efinance libraryencapsulation）
    positionlibrary：https://github.com/Micro-sheep/efinance
    
    main API：
    - ef.stock.get_quote_history(): get historical K linedata
    - ef.stock.get_base_info(): getstockbasicinfo
    - ef.stock.get_realtime_quotes(): get realtimequote/market data
    
    key strategy：
    - each timerequestrandom sleep before 1.5-3.0 seconds
    - random User-Agent rotate
    - failedafterindexbackoffretry（at most3times）
    """
    
    name = "EfinanceFetcher"
    priority = int(os.getenv("EFINANCE_PRIORITY", "0"))  # highest priority，rankin AkshareFetcher before
    
    def __init__(self, sleep_min: float = 1.5, sleep_max: float = 3.0):
        """
        initializing EfinanceFetcher
        
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

    @staticmethod
    def _build_history_failure_message(
        stock_code: str,
        beg_date: str,
        end_date: str,
        exc: Exception,
        elapsed: float,
        is_etf: bool = False,
    ) -> Tuple[str, str]:
        category, detail = _classify_eastmoney_error(exc)
        instrument_type = "ETF" if is_etf else "stock"
        message = (
            "Eastmoney historicalcandlestickAPI/interfacefailed: "
            f"endpoint={EASTMONEY_HISTORY_ENDPOINT}, stock_code={stock_code}, "
            f"market_type={instrument_type}, range={beg_date}~{end_date}, "
            f"category={category}, error_type={type(exc).__name__}, elapsed={elapsed:.2f}s, detail={detail}"
        )
        return category, message

    def _set_random_user_agent(self) -> None:
        """
        settingsrandom User-Agent
        
        viamodify requests Session  headers implement
        thisiskey anti-scrapingstrategyone of
        """
        try:
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
        stop=stop_after_attempt(1),  # reduceto1times，avoid triggeringrate limiting
        wait=wait_exponential(multiplier=1, min=4, max=60),  # maintainwait timesettings
        retry=retry_if_exception_type((
            ConnectionError,
            TimeoutError,
            requests.exceptions.RequestException,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        from efinance get rawdata
        
        based on codetypeautomaticselect API：
        - US stock：not supported，raiseabnormallet DataFetcherManager switch to otherdatasource
        - normalstock：use ef.stock.get_quote_history()
        - ETF fund：use ef.stock.get_quote_history()（ETF istradeplacesecurities，usestock K lineAPI/interface）
        
        process：
        1. determinecodetype（US stock/stock/ETF）
        2. settingsrandom User-Agent
        3. execution rateconstraint（random sleep）
        4. calltoshould efinance API
        5. processingreturndata
        """
        # US stocknot supported，raiseabnormallet DataFetcherManager switchto AkshareFetcher/YfinanceFetcher
        if _is_us_code(stock_code):
            raise DataFetchError(f"EfinanceFetcher not supportedUS stock {stock_code}，please use AkshareFetcher or YfinanceFetcher")
        
        # based on codetypechoose different fetchmethod
        if _is_etf_code(stock_code):
            return self._fetch_etf_data(stock_code, start_date, end_date)
        else:
            return self._fetch_stock_data(stock_code, start_date, end_date)
    
    def _fetch_stock_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        get normal A stock historicaldata
        
        Data sources:ef.stock.get_quote_history()
        
        API parameter description：
        - stock_codes: stock code
        - beg: startingdate，format 'YYYYMMDD'
        - end: end date，format 'YYYYMMDD'
        - klt: period，101=daily line
        - fqt: adjustedmethod，1=forward adjusted
        """
        import efinance as ef
        
        # anti-blocking strategy 1: random User-Agent
        self._set_random_user_agent()
        
        # anti-blocking strategy 2: mandatorysleep
        self._enforce_rate_limit()
        
        # formattingdate（efinance use YYYYMMDD format）
        beg_date = start_date.replace('-', '')
        end_date_fmt = end_date.replace('-', '')
        
        logger.info(f"[APIcall] ef.stock.get_quote_history(stock_codes={stock_code}, "
                   f"beg={beg_date}, end={end_date_fmt}, klt=101, fqt=1)")
        
        api_start = time.time()
        try:
            # call efinance get A stocksdaily linedata
            # klt=101 get daily datadata
            # fqt=1 getforward adjusteddata
            df = _ef_call_with_timeout(
                ef.stock.get_quote_history,
                stock_codes=stock_code,
                beg=beg_date,
                end=end_date_fmt,
                klt=101,  # daily line
                fqt=1,    # forward adjusted
                timeout=60,
            )
            
            api_elapsed = time.time() - api_start
            
            # recordreturndatasummary
            if df is not None and not df.empty:
                logger.info(
                    "[APIreturn] Eastmoney historicalcandlesticksuccessful: "
                    f"endpoint={EASTMONEY_HISTORY_ENDPOINT}, stock_code={stock_code}, "
                    f"range={beg_date}~{end_date_fmt}, rows={len(df)}, elapsed={api_elapsed:.2f}s"
                )
                logger.info(f"[APIreturn] column name: {list(df.columns)}")
                if 'date' in df.columns:
                    logger.info(f"[APIreturn] daterange: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
                logger.debug(f"[APIreturn] latest3itemsdata:\n{df.tail(3).to_string()}")
            else:
                logger.warning(
                    "[APIreturn] Eastmoney historicalcandlestickis empty: "
                    f"endpoint={EASTMONEY_HISTORY_ENDPOINT}, stock_code={stock_code}, "
                    f"range={beg_date}~{end_date_fmt}, elapsed={api_elapsed:.2f}s"
                )
            
            return df
            
        except Exception as e:
            api_elapsed = time.time() - api_start
            category, failure_message = self._build_history_failure_message(
                stock_code=stock_code,
                beg_date=beg_date,
                end_date=end_date_fmt,
                exc=e,
                elapsed=api_elapsed,
            )

            if category == "rate_limit_or_anti_bot":
                logger.warning(failure_message)
                raise RateLimitError(f"efinance may berate limiting: {failure_message}") from e

            logger.error(failure_message)
            raise DataFetchError(f"efinance getdatafailed: {failure_message}") from e
    
    def _fetch_etf_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        get ETF fundhistoricaldata

        Exchange-traded ETFs have OHLCV data just like regular stocks, so we use
        ef.stock.get_quote_history (the stock K-line API) which returns full
        open/high/low/close/volume data.

        Previously this method used ef.fund.get_quote_history which only returns
        NAV data (singledigitnetvalue/cumulativenetvalue) without volume or OHLC, causing:
        - Issue #541: 'got an unexpected keyword argument beg'
        - Issue #527: ETF volume/turnover always showing 0

        Args:
            stock_code: ETF code, e.g. '512400', '159883', '515120'
            start_date: Start date, format 'YYYY-MM-DD'
            end_date: End date, format 'YYYY-MM-DD'

        Returns:
            ETF historical OHLCV DataFrame
        """
        import efinance as ef

        # Anti-ban strategy 1: random User-Agent
        self._set_random_user_agent()

        # Anti-ban strategy 2: enforce rate limit
        self._enforce_rate_limit()

        # Format dates (efinance uses YYYYMMDD)
        beg_date = start_date.replace('-', '')
        end_date_fmt = end_date.replace('-', '')

        logger.info(f"[APIcall] ef.stock.get_quote_history(stock_codes={stock_code}, "
                     f"beg={beg_date}, end={end_date_fmt}, klt=101, fqt=1)  [ETF]")

        api_start = time.time()
        try:
            # ETFs are exchange-traded securities; use the stock API to get full OHLCV data
            df = _ef_call_with_timeout(
                ef.stock.get_quote_history,
                stock_codes=stock_code,
                beg=beg_date,
                end=end_date_fmt,
                klt=101,  # daily
                fqt=1,    # forward-adjusted
                timeout=60,
            )

            api_elapsed = time.time() - api_start

            if df is not None and not df.empty:
                logger.info(
                    "[APIreturn] Eastmoney historicalcandlesticksuccessful [ETF]: "
                    f"endpoint={EASTMONEY_HISTORY_ENDPOINT}, stock_code={stock_code}, "
                    f"range={beg_date}~{end_date_fmt}, rows={len(df)}, elapsed={api_elapsed:.2f}s"
                )
                logger.info(f"[APIreturn] column name: {list(df.columns)}")
                if 'date' in df.columns:
                    logger.info(f"[APIreturn] daterange: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}")
                logger.debug(f"[APIreturn] latest3itemsdata:\n{df.tail(3).to_string()}")
            else:
                logger.warning(
                    "[APIreturn] Eastmoney historicalcandlestickis empty [ETF]: "
                    f"endpoint={EASTMONEY_HISTORY_ENDPOINT}, stock_code={stock_code}, "
                    f"range={beg_date}~{end_date_fmt}, elapsed={api_elapsed:.2f}s"
                )

            return df

        except Exception as e:
            api_elapsed = time.time() - api_start
            category, failure_message = self._build_history_failure_message(
                stock_code=stock_code,
                beg_date=beg_date,
                end_date=end_date_fmt,
                exc=e,
                elapsed=api_elapsed,
                is_etf=True,
            )

            if category == "rate_limit_or_anti_bot":
                logger.warning(failure_message)
                raise RateLimitError(f"efinance may berate limiting: {failure_message}") from e

            logger.error(failure_message)
            raise DataFetchError(f"efinance get ETF datafailed: {failure_message}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        standardize efinance data
        
        efinance returned column names（Chinese）：
        stockname, stock code, date, open, close, highest, lowest, trading volume, trading amount, amplitude, price change percentage, price change amount, turnover rate
        
        need to map to standard column names：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()
        
        # Column mapping (efinance Chinese column names -> standard English column names)
        column_mapping = {
            'date': 'date',
            'open': 'open',
            'close': 'close',
            'highest': 'high',
            'lowest': 'low',
            'trading volume': 'volume',
            'trading amount': 'amount',
            'price change percentage': 'pct_chg',
            'stock code': 'code',
            'stockname': 'name',
        }
        
        # renamenamecolumn
        df = df.rename(columns=column_mapping)
        
        # Fallback: if OHLC columns are missing (e.g. very old data path), fill from close
        if 'close' in df.columns and 'open' not in df.columns:
            df['open'] = df['close']
            df['high'] = df['close']
            df['low'] = df['close']
            
        # Fill volume and amount if missing
        if 'volume' not in df.columns:
            df['volume'] = 0
        if 'amount' not in df.columns:
            df['amount'] = 0

        
        # if no code column，manualadd
        if 'code' not in df.columns:
            df['code'] = stock_code
        
        # keep only needed columns
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df
    
    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        get realtimequote/market datadata
        
        Data sources:ef.stock.get_realtime_quotes()
        ETF datasource：ef.stock.get_realtime_quotes(['ETF'])
        
        Args:
            stock_code: stock code
            
        Returns:
            UnifiedRealtimeQuote object，fetch failedreturn None
        """
        # ETF needseparaterequest ETF realtimequote/market dataAPI/interface
        if _is_etf_code(stock_code):
            return self._get_etf_realtime_quote(stock_code)

        import efinance as ef
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "efinance"
        
        # checkcircuit breakerstatus
        if not circuit_breaker.is_available(source_key):
            logger.warning(f"[circuit break] datasource {source_key} atcircuit breakstatus，skip")
            return None
        
        try:
            # checkcache
            current_time = time.time()
            if (_realtime_cache['data'] is not None and 
                current_time - _realtime_cache['timestamp'] < _realtime_cache['ttl']):
                df = _realtime_cache['data']
                cache_age = int(current_time - _realtime_cache['timestamp'])
                logger.debug(f"[cachehit] realtimequote/market data(efinance) - cacheage {cache_age}s/{_realtime_cache['ttl']}s")
            else:
                # trigger fullrefresh
                logger.info(f"[cachenothit] trigger fullrefresh realtimequote/market data(efinance)")
                # anti-blocking strategy
                self._set_random_user_agent()
                self._enforce_rate_limit()
                
                logger.info(f"[APIcall] ef.stock.get_realtime_quotes() get realtimequote/market data...")
                import time as _time
                api_start = _time.time()
                
                # efinance realtimequote/market data API (with timeout to avoid indefinite hangs)
                df = _ef_call_with_timeout(ef.stock.get_realtime_quotes)
                
                api_elapsed = _time.time() - api_start
                logger.info(f"[APIreturn] ef.stock.get_realtime_quotes successful: return {len(df)} onlystock, elapsed {api_elapsed:.2f}s")
                circuit_breaker.record_success(source_key)
                
                # updatingcache
                _realtime_cache['data'] = df
                _realtime_cache['timestamp'] = current_time
                logger.info(f"[cacheupdating] realtimequote/market data(efinance) cachealreadyrefresh，TTL={_realtime_cache['ttl']}s")
            
            # find specifiedstock
            # efinance returned column namespossiblyis 'stock code' or 'code'
            code_col = 'stock code' if 'stock code' in df.columns else 'code'
            row = df[df[code_col] == stock_code]
            if row.empty:
                logger.warning(f"[APIreturn] not foundstock {stock_code} realtimequote/market data")
                return None
            
            row = row.iloc[0]
            
            # use realtime_types.py unified inconvertingfunction
            # getcolumn name（possiblyisChineseorEnglish）
            name_col = 'stockname' if 'stockname' in df.columns else 'name'
            price_col = 'latest price' if 'latest price' in df.columns else 'price'
            pct_col = 'price change percentage' if 'price change percentage' in df.columns else 'pct_chg'
            chg_col = 'price change amount' if 'price change amount' in df.columns else 'change'
            vol_col = 'trading volume' if 'trading volume' in df.columns else 'volume'
            amt_col = 'trading amount' if 'trading amount' in df.columns else 'amount'
            turn_col = 'turnover rate' if 'turnover rate' in df.columns else 'turnover_rate'
            amp_col = 'amplitude' if 'amplitude' in df.columns else 'amplitude'
            high_col = 'highest' if 'highest' in df.columns else 'high'
            low_col = 'lowest' if 'lowest' in df.columns else 'low'
            open_col = 'open' if 'open' in df.columns else 'open'
            # efinance alsoreturnvolume ratio、P/E ratio、marketvalueetcfield
            vol_ratio_col = 'volume ratio' if 'volume ratio' in df.columns else 'volume_ratio'
            pe_col = 'P/E ratio' if 'P/E ratio' in df.columns else 'pe_ratio'
            total_mv_col = 'total market cap' if 'total market cap' in df.columns else 'total_mv'
            circ_mv_col = 'circulating market cap' if 'circulating market cap' in df.columns else 'circ_mv'
            
            quote = UnifiedRealtimeQuote(
                code=stock_code,
                name=str(row.get(name_col, '')),
                source=RealtimeSource.EFINANCE,
                price=safe_float(row.get(price_col)),
                change_pct=safe_float(row.get(pct_col)),
                change_amount=safe_float(row.get(chg_col)),
                volume=safe_int(row.get(vol_col)),
                amount=safe_float(row.get(amt_col)),
                turnover_rate=safe_float(row.get(turn_col)),
                amplitude=safe_float(row.get(amp_col)),
                high=safe_float(row.get(high_col)),
                low=safe_float(row.get(low_col)),
                open_price=safe_float(row.get(open_col)),
                volume_ratio=safe_float(row.get(vol_ratio_col)),  # volume ratio
                pe_ratio=safe_float(row.get(pe_col)),  # P/E ratio
                total_mv=safe_float(row.get(total_mv_col)),  # total market cap
                circ_mv=safe_float(row.get(circ_mv_col)),  # circulating market cap
            )
            
            logger.info(f"[realtimequote/market data-efinance] {stock_code} {quote.name}: price={quote.price}, price change={quote.change_pct}%, "
                       f"volume ratio={quote.volume_ratio}, turnover rate={quote.turnover_rate}%")
            return quote
            
        except FuturesTimeoutError:
            logger.warning(f"[timeout] ef.stock.get_realtime_quotes() exceed {_EF_CALL_TIMEOUT}s，skip {stock_code}")
            circuit_breaker.record_failure(source_key, "timeout")
            return None
        except Exception as e:
            logger.error(f"[APIerror] get {stock_code} realtimequote/market data(efinance)failed: {e}")
            circuit_breaker.record_failure(source_key, str(e))
            return None

    def _get_etf_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        get ETF realtimequote/market data

        efinance defaultrealtimeAPI/interfaceonlyreturnstockdata，ETF needexplicitly pass ['ETF']。
        """
        import efinance as ef
        circuit_breaker = get_realtime_circuit_breaker()
        source_key = "efinance_etf"

        if not circuit_breaker.is_available(source_key):
            logger.warning(f"[circuit break] datasource {source_key} atcircuit breakstatus，skip")
            return None

        try:
            current_time = time.time()
            if (
                _etf_realtime_cache['data'] is not None and
                current_time - _etf_realtime_cache['timestamp'] < _etf_realtime_cache['ttl']
            ):
                df = _etf_realtime_cache['data']
                cache_age = int(current_time - _etf_realtime_cache['timestamp'])
                logger.debug(f"[cachehit] ETFrealtimequote/market data(efinance) - cacheage {cache_age}s/{_etf_realtime_cache['ttl']}s")
            else:
                self._set_random_user_agent()
                self._enforce_rate_limit()

                logger.info("[APIcall] ef.stock.get_realtime_quotes(['ETF']) getETFrealtimequote/market data...")
                import time as _time
                api_start = _time.time()
                df = _ef_call_with_timeout(ef.stock.get_realtime_quotes, ['ETF'])
                api_elapsed = _time.time() - api_start

                if df is not None and not df.empty:
                    logger.info(f"[APIreturn] ETF realtimequote/market datasuccessful: {len(df)} items, elapsed {api_elapsed:.2f}s")
                    circuit_breaker.record_success(source_key)
                else:
                    logger.warning(f"[APIreturn] ETF realtimequote/market datais empty, elapsed {api_elapsed:.2f}s")
                    df = pd.DataFrame()

                _etf_realtime_cache['data'] = df
                _etf_realtime_cache['timestamp'] = current_time

            if df is None or df.empty:
                logger.warning(f"[realtimequote/market data] ETFrealtimequote/market datadata is empty(efinance)，skip {stock_code}")
                return None

            code_col = 'stock code' if 'stock code' in df.columns else 'code'
            code_series = df[code_col].astype(str).str.zfill(6)
            target_code = str(stock_code).strip().zfill(6)
            row = df[code_series == target_code]
            if row.empty:
                logger.warning(f"[APIreturn] not found ETF {stock_code} realtimequote/market data(efinance)")
                return None

            row = row.iloc[0]
            name_col = 'stockname' if 'stockname' in df.columns else 'name'
            price_col = 'latest price' if 'latest price' in df.columns else 'price'
            pct_col = 'price change percentage' if 'price change percentage' in df.columns else 'pct_chg'
            chg_col = 'price change amount' if 'price change amount' in df.columns else 'change'
            vol_col = 'trading volume' if 'trading volume' in df.columns else 'volume'
            amt_col = 'trading amount' if 'trading amount' in df.columns else 'amount'
            turn_col = 'turnover rate' if 'turnover rate' in df.columns else 'turnover_rate'
            amp_col = 'amplitude' if 'amplitude' in df.columns else 'amplitude'
            high_col = 'highest' if 'highest' in df.columns else 'high'
            low_col = 'lowest' if 'lowest' in df.columns else 'low'
            open_col = 'open' if 'open' in df.columns else 'open'

            quote = UnifiedRealtimeQuote(
                code=target_code,
                name=str(row.get(name_col, '')),
                source=RealtimeSource.EFINANCE,
                price=safe_float(row.get(price_col)),
                change_pct=safe_float(row.get(pct_col)),
                change_amount=safe_float(row.get(chg_col)),
                volume=safe_int(row.get(vol_col)),
                amount=safe_float(row.get(amt_col)),
                turnover_rate=safe_float(row.get(turn_col)),
                amplitude=safe_float(row.get(amp_col)),
                high=safe_float(row.get(high_col)),
                low=safe_float(row.get(low_col)),
                open_price=safe_float(row.get(open_col)),
            )

            logger.info(
                f"[ETFrealtimequote/market data-efinance] {target_code} {quote.name}: "
                f"price={quote.price}, price change={quote.change_pct}%, turnover rate={quote.turnover_rate}%"
            )
            return quote
        except Exception as e:
            logger.error(f"[APIerror] get ETF {stock_code} realtimequote/market data(efinance)failed: {e}")
            circuit_breaker.record_failure(source_key, str(e))
            return None

    def get_main_indices(self, region: str = "cn") -> Optional[List[Dict[str, Any]]]:
        """
        get mainindexrealtimequote/market data (efinance)，only support A stocks
        """
        if region != "cn":
            return None
        import efinance as ef

        indices_map = {
            '000001': ('Shanghai Compositeindex', 'sh000001'),
            '399001': ('Shenzhen Component Index', 'sz399001'),
            '399006': ('ChiNextrefers to', 'sz399006'),
            '000688': ('STAR50', 'sh000688'),
            '000016': ('Shanghai Composite50', 'sh000016'),
            '000300': ('Shanghai-Shenzhen300', 'sh000300'),
        }

        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info("[APIcall] ef.stock.get_realtime_quotes(['Shanghai-Shenzhensystemcolumnindex']) getindexquote/market data...")
            import time as _time
            api_start = _time.time()
            df = _ef_call_with_timeout(ef.stock.get_realtime_quotes, ['Shanghai-Shenzhensystemcolumnindex'])
            api_elapsed = _time.time() - api_start

            if df is None or df.empty:
                logger.warning(f"[APIreturn] indexquote/market datais empty, elapsed {api_elapsed:.2f}s")
                return None

            logger.info(f"[APIreturn] indexquote/market datasuccessful: {len(df)} items, elapsed {api_elapsed:.2f}s")
            code_col = 'stock code' if 'stock code' in df.columns else 'code'
            code_series = df[code_col].astype(str).str.zfill(6)

            results: List[Dict[str, Any]] = []
            for code, (name, full_code) in indices_map.items():
                row = df[code_series == code]
                if row.empty:
                    continue
                item = row.iloc[0]

                price_col = 'latest price' if 'latest price' in df.columns else 'price'
                pct_col = 'price change percentage' if 'price change percentage' in df.columns else 'pct_chg'
                chg_col = 'price change amount' if 'price change amount' in df.columns else 'change'
                open_col = 'open' if 'open' in df.columns else 'open'
                high_col = 'highest' if 'highest' in df.columns else 'high'
                low_col = 'lowest' if 'lowest' in df.columns else 'low'
                vol_col = 'trading volume' if 'trading volume' in df.columns else 'volume'
                amt_col = 'trading amount' if 'trading amount' in df.columns else 'amount'
                amp_col = 'amplitude' if 'amplitude' in df.columns else 'amplitude'

                current = safe_float(item.get(price_col, 0))
                change_amount = safe_float(item.get(chg_col, 0))

                results.append({
                    'code': full_code,
                    'name': name,
                    'current': current,
                    'change': change_amount,
                    'change_pct': safe_float(item.get(pct_col, 0)),
                    'open': safe_float(item.get(open_col, 0)),
                    'high': safe_float(item.get(high_col, 0)),
                    'low': safe_float(item.get(low_col, 0)),
                    'prev_close': current - change_amount if current or change_amount else 0,
                    'volume': safe_float(item.get(vol_col, 0)),
                    'amount': safe_float(item.get(amt_col, 0)),
                    'amplitude': safe_float(item.get(amp_col, 0)),
                })

            if results:
                logger.info(f"[efinance] getto {len(results)} countindexquote/market data")
            return results if results else None
        except Exception as e:
            logger.error(f"[efinance] getindexquote/market datafailed: {e}")
            return None

    def get_market_stats(self) -> Optional[Dict[str, Any]]:
        """
        get market gain/loss statistics (efinance)
        """
        import efinance as ef

        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            current_time = time.time()
            if (
                _realtime_cache['data'] is not None and
                current_time - _realtime_cache['timestamp'] < _realtime_cache['ttl']
            ):
                df = _realtime_cache['data']
            else:
                logger.info("[APIcall] ef.stock.get_realtime_quotes() get market statistics...")
                df = _ef_call_with_timeout(ef.stock.get_realtime_quotes)
                _realtime_cache['data'] = df
                _realtime_cache['timestamp'] = current_time

            if df is None or df.empty:
                logger.warning("[APIreturn] market statisticsdata is empty")
                return None

            return self._calc_market_stats(df)
        except Exception as e:
            logger.error(f"[efinance] get market statisticsfailed: {e}")
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
        getsectorgain/loss rankings (efinance)
        """
        import efinance as ef

        try:
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info("[APIcall] ef.stock.get_realtime_quotes(['industrysector']) getsectorquote/market data...")
            df = _ef_call_with_timeout(ef.stock.get_realtime_quotes, ['industrysector'])
            if df is None or df.empty:
                logger.warning("[efinance] sectorquote/market datadata is empty")
                return None

            change_col = 'price change percentage' if 'price change percentage' in df.columns else 'pct_chg'
            name_col = 'stockname' if 'stockname' in df.columns else 'name'
            if change_col not in df.columns or name_col not in df.columns:
                return None

            df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
            df = df.dropna(subset=[change_col])
            top = df.nlargest(n, change_col)
            bottom = df.nsmallest(n, change_col)

            top_sectors = [
                {'name': str(row[name_col]), 'change_pct': float(row[change_col])}
                for _, row in top.iterrows()
            ]
            bottom_sectors = [
                {'name': str(row[name_col]), 'change_pct': float(row[change_col])}
                for _, row in bottom.iterrows()
            ]
            return top_sectors, bottom_sectors
        except Exception as e:
            logger.error(f"[efinance] getsectorrankingfailed: {e}")
            return None
    
    def get_base_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        getstockbasicinfo
        
        Data sources:ef.stock.get_base_info()
        packageinclude：P/E ratio、P/B ratio、placeatindustry、total market cap、circulating market cap、ROE、net marginetc
        
        Args:
            stock_code: stock code
            
        Returns:
            packageincludebasicinfodictionary，fetch failedreturn None
        """
        import efinance as ef
        
        try:
            # anti-blocking strategy
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            logger.info(f"[APIcall] ef.stock.get_base_info(stock_codes={stock_code}) getbasicinfo...")
            import time as _time
            api_start = _time.time()
            
            info = _ef_call_with_timeout(ef.stock.get_base_info, stock_code)
            
            api_elapsed = _time.time() - api_start
            logger.info(f"[APIreturn] ef.stock.get_base_info successful, elapsed {api_elapsed:.2f}s")
            
            if info is None:
                logger.warning(f"[APIreturn] failed to get {stock_code} basicinfo")
                return None
            
            # convertingasdictionary
            if isinstance(info, pd.Series):
                return info.to_dict()
            elif isinstance(info, pd.DataFrame):
                if not info.empty:
                    return info.iloc[0].to_dict()
            
            return None
            
        except Exception as e:
            logger.error(f"[APIerror] get {stock_code} basicinfofailed: {e}")
            return None
    
    def get_belong_board(self, stock_code: str) -> Optional[pd.DataFrame]:
        """
        getstockbelonging tosector
        
        Data sources:ef.stock.get_belong_board()
        
        Args:
            stock_code: stock code
            
        Returns:
            belonging tosector DataFrame，fetch failedreturn None
        """
        import efinance as ef
        
        try:
            # anti-blocking strategy
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            logger.info(f"[APIcall] ef.stock.get_belong_board(stock_code={stock_code}) get belongingsector...")
            import time as _time
            api_start = _time.time()
            
            df = _ef_call_with_timeout(ef.stock.get_belong_board, stock_code)
            
            api_elapsed = _time.time() - api_start
            
            if df is not None and not df.empty:
                logger.info(f"[APIreturn] ef.stock.get_belong_board successful: return {len(df)} countsector, elapsed {api_elapsed:.2f}s")
                return df
            else:
                logger.warning(f"[APIreturn] failed to get {stock_code} sectorinfo")
                return None
            
        except FuturesTimeoutError:
            logger.warning(f"[timeout] ef.stock.get_belong_board({stock_code}) exceed {_EF_CALL_TIMEOUT}s，skip")
            return None
        except Exception as e:
            logger.error(f"[APIerror] get {stock_code} belonging tosectorfailed: {e}")
            return None
    
    def get_enhanced_data(self, stock_code: str, days: int = 60) -> Dict[str, Any]:
        """
        get enhanceddata（historicalcandlestick + realtimequote/market data + basicinfo）
        
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
            'base_info': None,
            'belong_board': None,
        }
        
        # get daily datadata
        try:
            df = self.get_daily_data(stock_code, days=days)
            result['daily_data'] = df
        except Exception as e:
            logger.error(f"get {stock_code} daily linedatafailed: {e}")
        
        # get realtimequote/market data
        result['realtime_quote'] = self.get_realtime_quote(stock_code)
        
        # getbasicinfo
        result['base_info'] = self.get_base_info(stock_code)
        
        # get belongingsector
        result['belong_board'] = self.get_belong_board(stock_code)
        
        return result


if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = EfinanceFetcher()
    
    # testingnormalstock
    print("=" * 50)
    print("testingnormalstockdataget (efinance)")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('600519')  # Maotai
        print(f"[stock] fetch successful，total {len(df)} itemsdata")
        print(df.tail())
    except Exception as e:
        print(f"[stock] fetch failed: {e}")
    
    # testing ETF fund
    print("\n" + "=" * 50)
    print("testing ETF funddataget (efinance)")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('512400')  # hascolorleading stockETF
        print(f"[ETF] fetch successful，total {len(df)} itemsdata")
        print(df.tail())
    except Exception as e:
        print(f"[ETF] fetch failed: {e}")
    
    # testingrealtimequote/market data
    print("\n" + "=" * 50)
    print("testingrealtimequote/market dataget (efinance)")
    print("=" * 50)
    try:
        quote = fetcher.get_realtime_quote('600519')
        if quote:
            print(f"[realtimequote/market data] {quote.name}: price={quote.price}, price change percentage={quote.change_pct}%")
        else:
            print("[realtimequote/market data] failed to getdata")
    except Exception as e:
        print(f"[realtimequote/market data] fetch failed: {e}")
    
    # testingbasicinfo
    print("\n" + "=" * 50)
    print("testingbasicinfoget (efinance)")
    print("=" * 50)
    try:
        info = fetcher.get_base_info('600519')
        if info:
            print(f"[basicinfo] P/E ratio={info.get('P/E ratio(dynamic)', 'N/A')}, P/B ratio={info.get('P/B ratio', 'N/A')}")
        else:
            print("[basicinfo] failed to getdata")
    except Exception as e:
        print(f"[basicinfo] fetch failed: {e}")

    # testingmarket statistics 
    print("\n" + "=" * 50)
    print("Testing get_market_stats (efinance)")
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
