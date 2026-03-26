# -*- coding: utf-8 -*-
"""
===================================
analyzingservicelayer
===================================

Responsibilities:
1. encapsulationstockanalyzinglogic
2. call analyzer and pipeline executeanalyzing
3. savinganalysis resulttodatabase
"""

import logging
import uuid
from typing import Optional, Dict, Any

from src.repositories.analysis_repo import AnalysisRepository
from src.report_language import (
    get_sentiment_label,
    get_localized_stock_name,
    localize_operation_advice,
    localize_trend_prediction,
    normalize_report_language,
)

logger = logging.getLogger(__name__)


class AnalysisService:
    """
    analyzingservice
    
    encapsulationstockanalyzingrelatedbusinesslogic
    """
    
    def __init__(self):
        """initializinganalyzingservice"""
        self.repo = AnalysisRepository()
    
    def analyze_stock(
        self,
        stock_code: str,
        report_type: str = "detailed",
        force_refresh: bool = False,
        query_id: Optional[str] = None,
        send_notification: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        executestockanalyzing
        
        Args:
            stock_code: stock code
            report_type: report type (simple/detailed)
            force_refresh: whethermandatoryrefresh
            query_id: querying ID（optional）
            send_notification: whethersendingnotification（API triggerdefaultsending）
            
        Returns:
            analysis resultdictionary，packageinclude:
            - stock_code: stock code
            - stock_name: stockname
            - report: analysis report
        """
        try:
            # importanalyzingrelatedmodule
            from src.config import get_config
            from src.core.pipeline import StockAnalysisPipeline
            from src.enums import ReportType
            
            # generating query_id
            if query_id is None:
                query_id = uuid.uuid4().hex
            
            # getconfiguration
            config = get_config()
            
            # creatinganalyzingpipeline
            pipeline = StockAnalysisPipeline(
                config=config,
                query_id=query_id,
                query_source="api"
            )
            
            # determinereport type (API: simple/detailed/full/brief -> ReportType)
            rt = ReportType.from_str(report_type)
            
            # executeanalyzing
            result = pipeline.process_single_stock(
                code=stock_code,
                skip_analysis=False,
                single_stock_notify=send_notification,
                report_type=rt
            )
            
            if result is None:
                logger.warning(f"analyzingstock {stock_code} return empty result")
                return None
            
            # buildresponse
            return self._build_analysis_response(result, query_id, report_type=rt.value)
            
        except Exception as e:
            logger.error(f"analyzingstock {stock_code} failed: {e}", exc_info=True)
            return None
    
    def _build_analysis_response(
        self, 
        result: Any, 
        query_id: str,
        report_type: str = "detailed",
    ) -> Dict[str, Any]:
        """
        buildanalyzingresponse
        
        Args:
            result: AnalysisResult object
            query_id: querying ID
            report_type: normalizeafterreport type
            
        Returns:
            formattingresponsedictionary
        """
        # getsniper entry point
        sniper_points = {}
        if hasattr(result, 'get_sniper_points'):
            sniper_points = result.get_sniper_points() or {}
        
        # calculatingsentimentlabel
        report_language = normalize_report_language(getattr(result, "report_language", "zh"))
        sentiment_label = get_sentiment_label(result.sentiment_score, report_language)
        stock_name = get_localized_stock_name(getattr(result, "name", None), result.code, report_language)
        
        # buildreport structure
        report = {
            "meta": {
                "query_id": query_id,
                "stock_code": result.code,
                "stock_name": stock_name,
                "report_type": report_type,
                "report_language": report_language,
                "current_price": result.current_price,
                "change_pct": result.change_pct,
                "model_used": getattr(result, "model_used", None),
            },
            "summary": {
                "analysis_summary": result.analysis_summary,
                "operation_advice": localize_operation_advice(result.operation_advice, report_language),
                "trend_prediction": localize_trend_prediction(result.trend_prediction, report_language),
                "sentiment_score": result.sentiment_score,
                "sentiment_label": sentiment_label,
            },
            "strategy": {
                "ideal_buy": sniper_points.get("ideal_buy"),
                "secondary_buy": sniper_points.get("secondary_buy"),
                "stop_loss": sniper_points.get("stop_loss"),
                "take_profit": sniper_points.get("take_profit"),
            },
            "details": {
                "news_summary": result.news_summary,
                "technical_analysis": result.technical_analysis,
                "fundamental_analysis": result.fundamental_analysis,
                "risk_warning": result.risk_warning,
            }
        }
        
        return {
            "stock_code": result.code,
            "stock_name": stock_name,
            "report": report,
        }
