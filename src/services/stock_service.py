# -*- coding: utf-8 -*-
"""
===================================
stockdataservicelayer
===================================

Responsibilities:
1. encapsulationstockdatagetlogic
2. providerealtimequote/market dataand historicaldataAPI/interface
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from src.repositories.stock_repo import StockRepository

logger = logging.getLogger(__name__)


class StockService:
    """
    stockdataservice
    
    encapsulationstockdatagetbusinesslogic
    """
    
    def __init__(self):
        """initializingstockdataservice"""
        self.repo = StockRepository()
    
    def get_realtime_quote(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        getstockrealtimequote/market data
        
        Args:
            stock_code: stock code
            
        Returns:
            realtimequote/market datadatadictionary
        """
        try:
            # calldatagethandlerget realtimequote/market data
            from data_provider.base import DataFetcherManager
            
            manager = DataFetcherManager()
            quote = manager.get_realtime_quote(stock_code)
            
            if quote is None:
                logger.warning(f"get {stock_code} realtimequote/market datafailed")
                return None
            
            # UnifiedRealtimeQuote is dataclass，use getattr safeaccessfield
            # fieldmapping: UnifiedRealtimeQuote -> API response
            # - code -> stock_code
            # - name -> stock_name
            # - price -> current_price
            # - change_amount -> change
            # - change_pct -> change_percent
            # - open_price -> open
            # - high -> high
            # - low -> low
            # - pre_close -> prev_close
            # - volume -> volume
            # - amount -> amount
            return {
                "stock_code": getattr(quote, "code", stock_code),
                "stock_name": getattr(quote, "name", None),
                "current_price": getattr(quote, "price", 0.0) or 0.0,
                "change": getattr(quote, "change_amount", None),
                "change_percent": getattr(quote, "change_pct", None),
                "open": getattr(quote, "open_price", None),
                "high": getattr(quote, "high", None),
                "low": getattr(quote, "low", None),
                "prev_close": getattr(quote, "pre_close", None),
                "volume": getattr(quote, "volume", None),
                "amount": getattr(quote, "amount", None),
                "update_time": datetime.now().isoformat(),
            }
            
        except ImportError:
            logger.warning("DataFetcherManager not found，useaccount fordigitdata")
            return self._get_placeholder_quote(stock_code)
        except Exception as e:
            logger.error(f"get realtimequote/market datafailed: {e}", exc_info=True)
            return None
    
    def get_history_data(
        self,
        stock_code: str,
        period: str = "daily",
        days: int = 30
    ) -> Dict[str, Any]:
        """
        getstockhistoricalquote/market data
        
        Args:
            stock_code: stock code
            period: K line period (daily/weekly/monthly)
            days: get days count
            
        Returns:
            historicalquote/market datadatadictionary
            
        Raises:
            ValueError: when period is not daily raise when（weekly/monthly temporarilynotimplement）
        """
        # verification period parameter，onlysupport daily
        if period != "daily":
            raise ValueError(
                f"temporarilynot supported '{period}' period，itembeforeonly support 'daily'。"
                "weekly/monthly aggregationfeaturewillinaftercontinueversionimplement。"
            )
        
        try:
            # calldatagethandlerget historicaldata
            from data_provider.base import DataFetcherManager
            
            manager = DataFetcherManager()
            df, source = manager.get_daily_data(stock_code, days=days)
            
            if df is None or df.empty:
                logger.warning(f"get {stock_code} historicaldatafailed")
                return {"stock_code": stock_code, "period": period, "data": []}
            
            # getstockname
            stock_name = manager.get_stock_name(stock_code)
            
            # convertingasresponseformat
            data = []
            for _, row in df.iterrows():
                date_val = row.get("date")
                if hasattr(date_val, "strftime"):
                    date_str = date_val.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_val)
                
                data.append({
                    "date": date_str,
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", 0)) if row.get("volume") else None,
                    "amount": float(row.get("amount", 0)) if row.get("amount") else None,
                    "change_percent": float(row.get("pct_chg", 0)) if row.get("pct_chg") else None,
                })
            
            return {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "period": period,
                "data": data,
            }
            
        except ImportError:
            logger.warning("DataFetcherManager not found，return emptydata")
            return {"stock_code": stock_code, "period": period, "data": []}
        except Exception as e:
            logger.error(f"get historicaldatafailed: {e}", exc_info=True)
            return {"stock_code": stock_code, "period": period, "data": []}
    
    def _get_placeholder_quote(self, stock_code: str) -> Dict[str, Any]:
        """
        getaccount fordigitquote/market datadata（fortesting）
        
        Args:
            stock_code: stock code
            
        Returns:
            account fordigitquote/market datadata
        """
        return {
            "stock_code": stock_code,
            "stock_name": f"stock{stock_code}",
            "current_price": 0.0,
            "change": None,
            "change_percent": None,
            "open": None,
            "high": None,
            "low": None,
            "prev_close": None,
            "volume": None,
            "amount": None,
            "update_time": datetime.now().isoformat(),
        }
