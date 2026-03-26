# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - analyzingservicelayer
===================================

Responsibilities:
1. encapsulationcoreanalyzinglogic，support multiplecallmethod（CLI、WebUI、Bot）
2. provideclearAPIAPI/interface，notdependencyatcommandrowparameter
3. supportdependencyinject，for conveniencetestingandextension
4. unifiedmanageanalyzingprocessandconfiguration
"""

import uuid
from typing import List, Optional

from src.analyzer import AnalysisResult
from src.config import get_config, Config
from src.notification import NotificationService
from src.enums import ReportType
from src.core.pipeline import StockAnalysisPipeline
from src.core.market_review import run_market_review



def analyze_stock(
    stock_code: str,
    config: Config = None,
    full_report: bool = False,
    notifier: Optional[NotificationService] = None
) -> Optional[AnalysisResult]:
    """
    analyzingsinglestock
    
    Args:
        stock_code: stock code
        config: configurationobject（optional，defaultusesingleton）
        full_report: whethergeneratingFull Report
        notifier: notificationservice（optional）
        
    Returns:
        analysis resultobject
    """
    if config is None:
        config = get_config()
    
    # creatinganalyzingpipeline
    pipeline = StockAnalysisPipeline(
        config=config,
        query_id=uuid.uuid4().hex,
        query_source="cli"
    )
    
    # usenotificationservice（ifprovide）
    if notifier:
        pipeline.notifier = notifier
    
    # based onfull_reportparametersettingsreport type
    report_type = ReportType.FULL if full_report else ReportType.SIMPLE
    
    # runningsinglestockanalyzing
    result = pipeline.process_single_stock(
        code=stock_code,
        skip_analysis=False,
        single_stock_notify=notifier is not None,
        report_type=report_type
    )
    
    return result

def analyze_stocks(
    stock_codes: List[str],
    config: Config = None,
    full_report: bool = False,
    notifier: Optional[NotificationService] = None
) -> List[AnalysisResult]:
    """
    analyzingmultipleonlystock
    
    Args:
        stock_codes: stock codelist
        config: configurationobject（optional，defaultusesingleton）
        full_report: whethergeneratingFull Report
        notifier: notificationservice（optional）
        
    Returns:
        analysis resultlist
    """
    if config is None:
        config = get_config()
    
    results = []
    for stock_code in stock_codes:
        result = analyze_stock(stock_code, config, full_report, notifier)
        if result:
            results.append(result)
    
    return results

def perform_market_review(
    config: Config = None,
    notifier: Optional[NotificationService] = None
) -> Optional[str]:
    """
    execute market review
    
    Args:
        config: configurationobject（optional，defaultusesingleton）
        notifier: notificationservice（optional）
        
    Returns:
        reviewreport content
    """
    if config is None:
        config = get_config()
    
    # creatinganalyzingpipelinewithgetanalyzerandsearch_service
    pipeline = StockAnalysisPipeline(
        config=config,
        query_id=uuid.uuid4().hex,
        query_source="cli"
    )
    
    # useprovidenotificationserviceorcreatingnew
    review_notifier = notifier or pipeline.notifier
    
    # callmarket reviewfunction
    return run_market_review(
        notifier=review_notifier,
        analyzer=pipeline.analyzer,
        search_service=pipeline.search_service
    )


