# -*- coding: utf-8 -*-
"""
===================================
PytdxFetcher - TongDaXindatasource (Priority 2)
===================================

Data sources:TongDaXinquote/market dataservicehandler（pytdx library）
features：free、no need for Token、direct connectquote/market dataservicehandler
advantages：realtimedata、stable、noquotaconstraint

key strategy：
1. multipleservicehandlerauto switch
2. connection timeoutautomaticreconnect
3. failedafterindexbackoffretry
"""

import logging
import re
from contextlib import contextmanager
from typing import Optional, Generator, List, Tuple

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS, is_bse_code, _is_hk_market
import os

logger = logging.getLogger(__name__)


def _parse_hosts_from_env() -> Optional[List[Tuple[str, int]]]:
    """
    fromenvironment variablebuildTongDaXinservicehandlerlist。

    priority：
    1. PYTDX_SERVERS：comma-separated "ip:port,ip:port"（e.g. "192.168.1.1:7709,10.0.0.1:7709"）
    2. PYTDX_HOST + PYTDX_PORT：singleservicehandler
    3. averagenotconfigurationreturn when None（callmethoduse DEFAULT_HOSTS）
    """
    servers = os.getenv("PYTDX_SERVERS", "").strip()
    if servers:
        result = []
        for part in servers.split(","):
            part = part.strip()
            if ":" in part:
                host, port_str = part.rsplit(":", 1)
                host, port_str = host.strip(), port_str.strip()
                if host and port_str:
                    try:
                        result.append((host, int(port_str)))
                    except ValueError:
                        logger.warning(f"Invalid PYTDX_SERVERS entry: {part}")
            else:
                logger.warning(f"Invalid PYTDX_SERVERS entry (missing port): {part}")
        if result:
            return result

    host = os.getenv("PYTDX_HOST", "").strip()
    port_str = os.getenv("PYTDX_PORT", "").strip()
    if host and port_str:
        try:
            return [(host, int(port_str))]
        except ValueError:
            logger.warning(f"Invalid PYTDX_HOST/PYTDX_PORT: {host}:{port_str}")

    return None


def _is_us_code(stock_code: str) -> bool:
    """
    check if code isUS stock
    
    US stockcode rules：
    - 1-5uppercase letters，e.g. 'AAPL', 'TSLA'
    - possiblypackageinclude '.'，e.g. 'BRK.B'
    """
    code = stock_code.strip().upper()
    return bool(re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', code))


class PytdxFetcher(BaseFetcher):
    """
    TongDaXindatasource implementation
    
    priority：2（with Tushare samelevel）
    Data sources:TongDaXinquote/market dataservicehandler
    
    key strategy：
    - automaticselectoptimalservicehandler
    - connection failedauto switchservicehandler
    - failedafterindexbackoffretry
    
    Pytdx features：
    - free、no need forregister
    - direct connectquote/market dataservicehandler
    - supportrealtimequote/market dataand historicaldata
    - supportstocknamequerying
    """
    
    name = "PytdxFetcher"
    priority = int(os.getenv("PYTDX_PRIORITY", "2"))
    
    # defaultTongDaXinquote/market dataservicehandlerlist
    DEFAULT_HOSTS = [
        ("119.147.212.81", 7709),  # Shenzhen
        ("112.74.214.43", 7727),   # Shenzhen
        ("221.231.141.60", 7709),  # Shanghai
        ("101.227.73.20", 7709),   # Shanghai
        ("101.227.77.254", 7709),  # Shanghai
        ("14.215.128.18", 7709),   # Guangzhou
        ("59.173.18.140", 7709),   # Wuhan
        ("180.153.39.51", 7709),   # Hangzhou
    ]
    # Pytdx get_security_list returns at most 1000 items per page
    SECURITY_LIST_PAGE_SIZE = 1000
    
    def __init__(self, hosts: Optional[List[Tuple[str, int]]] = None):
        """
        initializing PytdxFetcher

        Args:
            hosts: servicehandlerlist [(host, port), ...]。ifnotpass in，prefer to useenvironment variable
                   PYTDX_SERVERS（ip:port,ip:port）or PYTDX_HOST+PYTDX_PORT，
                   otherwiseusebuilt-in DEFAULT_HOSTS。
        """
        if hosts is not None:
            self._hosts = hosts
        else:
            env_hosts = _parse_hosts_from_env()
            self._hosts = env_hosts if env_hosts else self.DEFAULT_HOSTS
        self._api = None
        self._connected = False
        self._current_host_idx = 0
        self._stock_list_cache = None  # stocklistcache
        self._stock_name_cache = {}    # stocknamecache {code: name}
    
    def _get_pytdx(self):
        """
        delayloading pytdx module
        
        onlyinfirsttimesusewhenimport，avoidnotsetupwhenerror report
        """
        try:
            from pytdx.hq import TdxHq_API
            return TdxHq_API
        except ImportError:
            logger.warning("pytdx notsetup，pleaserunning: pip install pytdx")
            return None
    
    @contextmanager
    def _pytdx_session(self) -> Generator:
        """
        Pytdx connectingcontextmanager
        
        ensure：
        1. entercontextautomatically whenconnecting
        2. logoutcontextautomatically whendisconnecting
        3. abnormalwhencan alsocorrectdisconnecting
        
        useExample：
            with self._pytdx_session() as api:
                # inhereexecutedataquerying
        """
        TdxHq_API = self._get_pytdx()
        if TdxHq_API is None:
            raise DataFetchError("pytdx librarynotsetup")
        
        api = TdxHq_API()
        connected = False
        
        try:
            # tryconnectingservicehandler（automaticselectoptimal）
            for i in range(len(self._hosts)):
                host_idx = (self._current_host_idx + i) % len(self._hosts)
                host, port = self._hosts[host_idx]
                
                try:
                    if api.connect(host, port, time_out=5):
                        connected = True
                        self._current_host_idx = host_idx
                        logger.debug(f"Pytdx connectingsuccessful: {host}:{port}")
                        break
                except Exception as e:
                    logger.debug(f"Pytdx connecting {host}:{port} failed: {e}")
                    continue
            
            if not connected:
                raise DataFetchError("Pytdx unable toconnectinganyservicehandler")
            
            yield api
            
        finally:
            # ensuredisconnectingconnecting
            try:
                api.disconnect()
                logger.debug("Pytdx connectingalreadydisconnecting")
            except Exception as e:
                logger.warning(f"Pytdx disconnectingconnectingwhenerror: {e}")
    
    def _get_market_code(self, stock_code: str) -> Tuple[int, str]:
        """
        based onstock codedeterminemarket
        
        Pytdx marketcode：
        - 0: Shenzhen
        - 1: Shanghai
        
        Args:
            stock_code: stock code
            
        Returns:
            (market, code) tuple
        """
        code = stock_code.strip()
        
        # removepossiblyprefixsuffix
        code = code.replace('.SH', '').replace('.SZ', '')
        code = code.replace('.sh', '').replace('.sz', '')
        code = code.replace('sh', '').replace('sz', '')
        
        # determine market by code prefix
        # Shanghai：60xxxx, 68xxxx（STAR Market）
        # Shenzhen：00xxxx, 30xxxx（ChiNext）, 002xxx（insmallboard）
        if code.startswith(('60', '68')):
            return 1, code  # Shanghai
        else:
            return 0, code  # Shenzhen

    def _build_stock_list_cache(self, api) -> None:
        """
        Build a full stock code -> name cache from paginated security lists.
        """
        self._stock_list_cache = {}

        for market in (0, 1):
            start = 0
            while True:
                stocks = api.get_security_list(market, start) or []
                for stock in stocks:
                    code = stock.get('code')
                    name = stock.get('name')
                    if code and name:
                        self._stock_list_cache[code] = name

                if len(stocks) < self.SECURITY_LIST_PAGE_SIZE:
                    break

                start += self.SECURITY_LIST_PAGE_SIZE
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        fromTongDaXinget rawdata
        
        use get_security_bars() get daily datadata
        
        process：
        1. checkwhether isUS stock（not supported）
        2. usecontextmanager managesconnecting
        3. determinemarketcode
        4. call API get K linedata
        """
        # US stocknot supported，raiseabnormallet DataFetcherManager switch to otherdatasource
        if _is_us_code(stock_code):
            raise DataFetchError(f"PytdxFetcher not supportedUS stock {stock_code}，please use AkshareFetcher or YfinanceFetcher")

        # HK stocknot supported，raiseabnormallet DataFetcherManager switch to otherdatasource
        if _is_hk_market(stock_code):
            raise DataFetchError(f"PytdxFetcher not supportedHK stock {stock_code}，please use AkshareFetcher")

        # BSEnot supported，raiseabnormallet DataFetcherManager switch to otherdatasource
        if is_bse_code(stock_code):
            raise DataFetchError(
                f"PytdxFetcher not supportedBSE {stock_code}，will automatically switch to otherdatasource"
            )
        
        market, code = self._get_market_code(stock_code)
        
        # calculatingneedgettrading dayquantity（estimate）
        from datetime import datetime as dt
        start_dt = dt.strptime(start_date, '%Y-%m-%d')
        end_dt = dt.strptime(end_date, '%Y-%m-%d')
        days = (end_dt - start_dt).days
        count = min(max(days * 5 // 7 + 10, 30), 800)  # estimatetrading day，max 800 items
        
        logger.debug(f"call Pytdx get_security_bars(market={market}, code={code}, count={count})")
        
        with self._pytdx_session() as api:
            try:
                # getday K linedata
                # category: 9-daily line, 0-5minutes, 1-15minutes, 2-30minutes, 3-1hours
                data = api.get_security_bars(
                    category=9,  # daily line
                    market=market,
                    code=code,
                    start=0,  # fromlateststarting
                    count=count
                )
                
                if data is None or len(data) == 0:
                    raise DataFetchError(f"Pytdx notqueryingto {stock_code} data")
                
                # convertingas DataFrame
                df = api.to_df(data)
                
                # filteringdaterange
                df['datetime'] = pd.to_datetime(df['datetime'])
                df = df[(df['datetime'] >= start_date) & (df['datetime'] <= end_date)]
                
                return df
                
            except Exception as e:
                if isinstance(e, DataFetchError):
                    raise
                raise DataFetchError(f"Pytdx getdatafailed: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        standardize Pytdx data
        
        Pytdx returned column names：
        datetime, open, high, low, close, vol, amount
        
        need to map to standard column names：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()
        
        # column name mapping
        column_mapping = {
            'datetime': 'date',
            'vol': 'volume',
        }
        
        df = df.rename(columns=column_mapping)
        
        # calculatingprice change percentage（pytdx notreturnprice change percentage，needselfselfcalculating）
        if 'pct_chg' not in df.columns and 'close' in df.columns:
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].fillna(0).round(2)
        
        # addstock codecolumn
        df['code'] = stock_code
        
        # keep only needed columns
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df
    
    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        getstockname
        
        Args:
            stock_code: stock code
            
        Returns:
            stockname，failedreturn None
        """
        # HK stocknot supported（pytdx notincludeHK stockdata）
        if _is_hk_market(stock_code):
            return None

        # firstcheckcache
        if stock_code in self._stock_name_cache:
            return self._stock_name_cache[stock_code]
        
        try:
            market, code = self._get_market_code(stock_code)
            
            with self._pytdx_session() as api:
                # getstocklist（cache）
                if self._stock_list_cache is None:
                    self._build_stock_list_cache(api)
                
                # findstockname
                name = self._stock_list_cache.get(code)
                if name:
                    self._stock_name_cache[stock_code] = name
                    return name
                
                # tryuse get_finance_info
                finance_info = api.get_finance_info(market, code)
                if finance_info and 'name' in finance_info:
                    name = finance_info['name']
                    self._stock_name_cache[stock_code] = name
                    return name
                
        except Exception as e:
            logger.warning(f"Pytdx getstocknamefailed {stock_code}: {e}")
        
        return None
    
    def get_realtime_quote(self, stock_code: str) -> Optional[dict]:
        """
        get realtimequote/market data
        
        Args:
            stock_code: stock code
            
        Returns:
            realtimequote/market datadatadictionary，failedreturn None
        """
        if is_bse_code(stock_code):
            raise DataFetchError(
                f"PytdxFetcher not supportedBSE {stock_code}，will automatically switch to otherdatasource"
            )
        try:
            market, code = self._get_market_code(stock_code)
            
            with self._pytdx_session() as api:
                data = api.get_security_quotes([(market, code)])
                
                if data and len(data) > 0:
                    quote = data[0]
                    return {
                        'code': stock_code,
                        'name': quote.get('name', ''),
                        'price': quote.get('price', 0),
                        'open': quote.get('open', 0),
                        'high': quote.get('high', 0),
                        'low': quote.get('low', 0),
                        'pre_close': quote.get('last_close', 0),
                        'volume': quote.get('vol', 0),
                        'amount': quote.get('amount', 0),
                        'bid_prices': [quote.get(f'bid{i}', 0) for i in range(1, 6)],
                        'ask_prices': [quote.get(f'ask{i}', 0) for i in range(1, 6)],
                    }
        except Exception as e:
            logger.warning(f"Pytdx get realtimequote/market datafailed {stock_code}: {e}")
        
        return None


if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = PytdxFetcher()
    
    try:
        # testinghistoricaldata
        df = fetcher.get_daily_data('600519')  # Maotai
        print(f"fetch successful，total {len(df)} itemsdata")
        print(df.tail())
        
        # testingstockname
        name = fetcher.get_stock_name('600519')
        print(f"stockname: {name}")
        
        # testingrealtimequote/market data
        quote = fetcher.get_realtime_quote('600519')
        print(f"realtimequote/market data: {quote}")
        
    except Exception as e:
        print(f"fetch failed: {e}")
