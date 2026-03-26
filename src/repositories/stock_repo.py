# -*- coding: utf-8 -*-
"""
===================================
stockdataaccess layer
===================================

Responsibilities:
1. encapsulationstockdatadatabaseoperation
2. providedaily linedataqueryingAPI/interface
"""

import logging
from datetime import date
from typing import Optional, List, Dict, Any

import pandas as pd
from sqlalchemy import and_, desc, select

from src.storage import DatabaseManager, StockDaily

logger = logging.getLogger(__name__)


class StockRepository:
    """
    stockdataaccess layer
    
    encapsulation StockDaily tabledatabaseoperation
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        initializingdataaccess layer
        
        Args:
            db_manager: databasemanager（optional，defaultusesingleton）
        """
        self.db = db_manager or DatabaseManager.get_instance()
    
    def get_latest(self, code: str, days: int = 2) -> List[StockDaily]:
        """
        get recent N daysdata
        
        Args:
            code: stock code
            days: get days count
            
        Returns:
            StockDaily objectlist（by datedescending）
        """
        try:
            return self.db.get_latest_data(code, days)
        except Exception as e:
            logger.error(f"get latestdatafailed: {e}")
            return []
    
    def get_range(
        self,
        code: str,
        start_date: date,
        end_date: date
    ) -> List[StockDaily]:
        """
        get specifieddaterangedata
        
        Args:
            code: stock code
            start_date: startingdate
            end_date: end date
            
        Returns:
            StockDaily objectlist
        """
        try:
            return self.db.get_data_range(code, start_date, end_date)
        except Exception as e:
            logger.error(f"getdaterangedatafailed: {e}")
            return []
    
    def save_dataframe(
        self,
        df: pd.DataFrame,
        code: str,
        data_source: str = "Unknown"
    ) -> int:
        """
        saving DataFrame todatabase
        
        Args:
            df: packageincludedaily linedata DataFrame
            code: stock code
            data_source: datasource
            
        Returns:
            savingrecordcount
        """
        try:
            return self.db.save_daily_data(df, code, data_source)
        except Exception as e:
            logger.error(f"savingdaily linedatafailed: {e}")
            return 0
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        checkwhetherhasspecifieddatedata
        
        Args:
            code: stock code
            target_date: target date（defaulttoday）
            
        Returns:
            whetherexistsdata
        """
        try:
            return self.db.has_today_data(code, target_date)
        except Exception as e:
            logger.error(f"checkdataexistsfailed: {e}")
            return False
    
    def get_analysis_context(
        self, 
        code: str, 
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        getanalyzingcontext
        
        Args:
            code: stock code
            target_date: target date
            
        Returns:
            analyzingcontextdictionary
        """
        try:
            return self.db.get_analysis_context(code, target_date)
        except Exception as e:
            logger.error(f"getanalyzingcontextfailed: {e}")
            return None

    def get_start_daily(self, *, code: str, analysis_date: date) -> Optional[StockDaily]:
        """Return StockDaily for analysis_date (preferred) or nearest previous date."""
        with self.db.get_session() as session:
            row = session.execute(
                select(StockDaily)
                .where(and_(StockDaily.code == code, StockDaily.date <= analysis_date))
                .order_by(desc(StockDaily.date))
                .limit(1)
            ).scalar_one_or_none()
            return row

    def get_forward_bars(self, *, code: str, analysis_date: date, eval_window_days: int) -> List[StockDaily]:
        """Return forward daily bars after analysis_date, up to eval_window_days."""
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily)
                .where(and_(StockDaily.code == code, StockDaily.date > analysis_date))
                .order_by(StockDaily.date)
                .limit(eval_window_days)
            ).scalars().all()
            return list(rows)
