# -*- coding: utf-8 -*-
"""
===================================
analyzinghistoricaldataaccess layer
===================================

Responsibilities:
1. encapsulationanalyzinghistoricaldatadatabaseoperation
2. provide CRUD API/interface
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from src.storage import DatabaseManager, AnalysisHistory

logger = logging.getLogger(__name__)


class AnalysisRepository:
    """
    analyzinghistoricaldataaccess layer
    
    encapsulation AnalysisHistory tabledatabaseoperation
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        initializingdataaccess layer
        
        Args:
            db_manager: databasemanager（optional，defaultusesingleton）
        """
        self.db = db_manager or DatabaseManager.get_instance()
    
    def get_by_query_id(self, query_id: str) -> Optional[AnalysisHistory]:
        """
        based on query_id getanalyzingrecord
        
        Args:
            query_id: querying ID
            
        Returns:
            AnalysisHistory object，not foundreturn None
        """
        try:
            records = self.db.get_analysis_history(query_id=query_id, limit=1)
            return records[0] if records else None
        except Exception as e:
            logger.error(f"queryinganalyzingrecordfailed: {e}")
            return None
    
    def get_list(
        self,
        code: Optional[str] = None,
        days: int = 30,
        limit: int = 50
    ) -> List[AnalysisHistory]:
        """
        getanalyzingrecordlist
        
        Args:
            code: stock codefilter
            days: timerange（days）
            limit: return countconstraint
            
        Returns:
            AnalysisHistory objectlist
        """
        try:
            return self.db.get_analysis_history(
                code=code,
                days=days,
                limit=limit
            )
        except Exception as e:
            logger.error(f"getanalyzinglistfailed: {e}")
            return []
    
    def save(
        self,
        result: Any,
        query_id: str,
        report_type: str,
        news_content: Optional[str] = None,
        context_snapshot: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        savinganalysis result
        
        Args:
            result: analysis resultobject
            query_id: querying ID
            report_type: report type
            news_content: newscontent
            context_snapshot: contextsnapshot
            
        Returns:
            savingrecordcount
        """
        try:
            return self.db.save_analysis_history(
                result=result,
                query_id=query_id,
                report_type=report_type,
                news_content=news_content,
                context_snapshot=context_snapshot
            )
        except Exception as e:
            logger.error(f"savinganalysis resultfailed: {e}")
            return 0
    
    def count_by_code(self, code: str, days: int = 30) -> int:
        """
        statisticsspecifiedstockanalyzingrecordcount
        
        Args:
            code: stock code
            days: timerange（days）
            
        Returns:
            recordquantity
        """
        try:
            records = self.db.get_analysis_history(code=code, days=days, limit=1000)
            return len(records)
        except Exception as e:
            logger.error(f"statisticsanalyzingrecordfailed: {e}")
            return 0
