# -*- coding: utf-8 -*-
"""
===================================
historicalrecordAPI/interface
===================================

Responsibilities:
1. provide GET /api/v1/history historicallistqueryingAPI/interface
2. provide GET /api/v1/history/{query_id} historicaldetailsqueryingAPI/interface
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Depends, Body

from api.deps import get_database_manager
from api.v1.schemas.history import (
    HistoryListResponse,
    HistoryItem,
    DeleteHistoryRequest,
    DeleteHistoryResponse,
    NewsIntelItem,
    NewsIntelResponse,
    AnalysisReport,
    ReportMeta,
    ReportSummary,
    ReportStrategy,
    ReportDetails,
    MarkdownReportResponse,
)
from api.v1.schemas.common import ErrorResponse
from src.storage import DatabaseManager
from src.report_language import (
    get_sentiment_label,
    get_localized_stock_name,
    localize_operation_advice,
    localize_trend_prediction,
    normalize_report_language,
)
from src.services.history_service import HistoryService, MarkdownReportGenerationError
from src.utils.data_processing import normalize_model_used, extract_fundamental_detail_fields

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "",
    response_model=HistoryListResponse,
    responses={
        200: {"description": "historicalrecordlist"},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="get historicalanalyzinglist",
    description="paginationget historicalanalyzingrecordsummary，support bystock codeanddaterangefilter"
)
def get_history_list(
    stock_code: Optional[str] = Query(None, description="stock codefilter"),
    start_date: Optional[str] = Query(None, description="startingdate (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="end date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="page number（from 1 starting）"),
    limit: int = Query(20, ge=1, le=100, description="items per page"),
    db_manager: DatabaseManager = Depends(get_database_manager)
) -> HistoryListResponse:
    """
    get historicalanalyzinglist
    
    paginationget historicalanalyzingrecordsummary，support bystock codeanddaterangefilter
    
    Args:
        stock_code: stock codefilter
        start_date: startingdate
        end_date: end date
        page: page number
        limit: items per page
        db_manager: databasemanagerdependency
        
    Returns:
        HistoryListResponse: historicalrecordlist
    """
    try:
        service = HistoryService(db_manager)
        
        # use def instead of async def，FastAPI automatically inthreadexecute in pool
        result = service.get_history_list(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            page=page,
            limit=limit
        )
        
        # convertingasresponsemodel
        items = [
            HistoryItem(
                id=item.get("id"),
                query_id=item.get("query_id", ""),
                stock_code=item.get("stock_code", ""),
                stock_name=item.get("stock_name"),
                report_type=item.get("report_type"),
                sentiment_score=item.get("sentiment_score"),
                operation_advice=item.get("operation_advice"),
                created_at=item.get("created_at")
            )
            for item in result.get("items", [])
        ]
        
        return HistoryListResponse(
            total=result.get("total", 0),
            page=page,
            limit=limit,
            items=items
        )
        
    except Exception as e:
        logger.error(f"queryinghistoricallistfailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"queryinghistoricallistfailed: {str(e)}"
            }
        )


@router.delete(
    "",
    response_model=DeleteHistoryResponse,
    responses={
        200: {"description": "delete successful"},
        400: {"description": "requestparametererror", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="deletinghistoricalanalyzingrecord",
    description="byhistoricalrecordprimary key ID batchdeletinganalyzinghistorical"
)
def delete_history_records(
    request: DeleteHistoryRequest = Body(...),
    db_manager: DatabaseManager = Depends(get_database_manager)
) -> DeleteHistoryResponse:
    """
    byprimary key ID batchdeletinghistoricalanalyzingrecord。
    """
    record_ids = sorted({record_id for record_id in request.record_ids if record_id is not None})
    if not record_ids:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_request",
                "message": "record_ids cannotis empty"
            }
        )

    try:
        service = HistoryService(db_manager)
        deleted = service.delete_history_records(record_ids)
        return DeleteHistoryResponse(deleted=deleted)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"deletinghistoricalrecordfailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"deletinghistoricalrecordfailed: {str(e)}"
            }
        )


@router.get(
    "/{record_id}",
    response_model=AnalysisReport,
    responses={
        200: {"description": "report details"},
        404: {"description": "reportnot found", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="get historicalreport details",
    description="based onanalyzinghistoricalrecord ID or query_id getcompletehistoricalanalysis report"
)
def get_history_detail(
    record_id: str,
    db_manager: DatabaseManager = Depends(get_database_manager)
) -> AnalysisReport:
    """
    get historicalreport details
    
    based onanalyzinghistoricalrecordprimary key ID or query_id getcompletehistoricalanalysis report。
    try firstbyprimary key ID（integer）querying，ifparameteris notlegalintegerthenby query_id querying。
    
    Args:
        record_id: analyzinghistoricalrecordprimary key ID（integer）or query_id（string）
        db_manager: databasemanagerdependency
        
    Returns:
        AnalysisReport: completeanalysis report
        
    Raises:
        HTTPException: 404 - reportnot found
    """
    try:
        service = HistoryService(db_manager)
        
        # Try integer ID first, fall back to query_id string lookup
        result = service.resolve_and_get_detail(record_id)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "not_found",
                    "message": f"not found id/query_id={record_id} analyzingrecord"
                }
            )
        
        # from context_snapshot extract frompriceinfo
        current_price = None
        change_pct = None
        context_snapshot = result.get("context_snapshot")
        if context_snapshot and isinstance(context_snapshot, dict):
            # tryfrom enhanced_context.realtime get
            enhanced_context = context_snapshot.get("enhanced_context") or {}
            realtime = enhanced_context.get("realtime") or {}
            current_price = realtime.get("price")
            change_pct = realtime.get("change_pct") or realtime.get("change_60d")
            
            # alsotryfrom realtime_quote_raw get
            if current_price is None:
                realtime_quote_raw = context_snapshot.get("realtime_quote_raw") or {}
                current_price = realtime_quote_raw.get("price")
                change_pct = change_pct or realtime_quote_raw.get("change_pct") or realtime_quote_raw.get("pct_chg")
        
        raw_result = result.get("raw_result")
        if not isinstance(raw_result, dict):
            raw_result = {}
        report_language = normalize_report_language(
            result.get("report_language")
            or raw_result.get("report_language")
            or (
                context_snapshot.get("report_language")
                if isinstance(context_snapshot, dict)
                else None
            )
        )
        stock_name = get_localized_stock_name(
            result.get("stock_name"),
            result.get("stock_code", ""),
            report_language,
        )

        # buildresponsemodel
        meta = ReportMeta(
            id=result.get("id"),
            query_id=result.get("query_id", ""),
            stock_code=result.get("stock_code", ""),
            stock_name=stock_name,
            report_type=result.get("report_type"),
            report_language=report_language,
            created_at=result.get("created_at"),
            current_price=current_price,
            change_pct=change_pct,
            model_used=normalize_model_used(result.get("model_used"))
        )
        
        summary = ReportSummary(
            analysis_summary=result.get("analysis_summary"),
            operation_advice=localize_operation_advice(
                result.get("operation_advice"),
                report_language,
            ),
            trend_prediction=localize_trend_prediction(
                result.get("trend_prediction"),
                report_language,
            ),
            sentiment_score=result.get("sentiment_score"),
            sentiment_label=(
                get_sentiment_label(result.get("sentiment_score"), report_language)
                if result.get("sentiment_score") is not None
                else result.get("sentiment_label")
            )
        )
        
        strategy = ReportStrategy(
            ideal_buy=result.get("ideal_buy"),
            secondary_buy=result.get("secondary_buy"),
            stop_loss=result.get("stop_loss"),
            take_profit=result.get("take_profit")
        )
        
        fallback_fundamental = db_manager.get_latest_fundamental_snapshot(
            query_id=result.get("query_id", ""),
            code=result.get("stock_code", ""),
        )
        extracted_fundamental = extract_fundamental_detail_fields(
            context_snapshot=result.get("context_snapshot"),
            fallback_fundamental_payload=fallback_fundamental,
        )

        details = ReportDetails(
            news_content=result.get("news_content"),
            raw_result=result.get("raw_result"),
            context_snapshot=result.get("context_snapshot"),
            financial_report=extracted_fundamental.get("financial_report"),
            dividend_metrics=extracted_fundamental.get("dividend_metrics"),
        )
        
        return AnalysisReport(
            meta=meta,
            summary=summary,
            strategy=strategy,
            details=details
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"queryinghistoricaldetailsfailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"queryinghistoricaldetailsfailed: {str(e)}"
            }
        )


@router.get(
    "/{record_id}/news",
    response_model=NewsIntelResponse,
    responses={
        200: {"description": "newsintelligencelist"},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="get historicalreportassociationnews",
    description="based onanalyzinghistoricalrecord ID getassociationnewsintelligencelist（is emptyalsoreturn 200）"
)
def get_history_news(
    record_id: str,
    limit: int = Query(20, ge=1, le=100, description="return countconstraint"),
    db_manager: DatabaseManager = Depends(get_database_manager)
) -> NewsIntelResponse:
    """
    get historicalreportassociationnews

    based onanalyzinghistoricalrecord ID or query_id getassociationnewsintelligencelist。
    ininternalcompleted record_id → query_id parsing。

    Args:
        record_id: analyzinghistoricalrecordprimary key ID（integer）or query_id（string）
        limit: return countconstraint
        db_manager: databasemanagerdependency

    Returns:
        NewsIntelResponse: newsintelligencelist
    """
    try:
        service = HistoryService(db_manager)
        items = service.resolve_and_get_news(record_id=record_id, limit=limit)

        response_items = [
            NewsIntelItem(
                title=item.get("title", ""),
                snippet=item.get("snippet"),
                url=item.get("url", "")
            )
            for item in items
        ]

        return NewsIntelResponse(
            total=len(response_items),
            items=response_items
        )

    except Exception as e:
        logger.error(f"queryingnewsintelligencefailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"queryingnewsintelligencefailed: {str(e)}"
            }
        )


@router.get(
    "/{record_id}/markdown",
    response_model=MarkdownReportResponse,
    responses={
        200: {"description": "Markdown formatreport"},
        404: {"description": "reportnot found", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="get historicalreport Markdown format",
    description="based onanalyzinghistoricalrecord ID get Markdown formatcompleteanalysis report"
)
def get_history_markdown(
    record_id: str,
    db_manager: DatabaseManager = Depends(get_database_manager)
) -> MarkdownReportResponse:
    """
    get historicalreport Markdown formatcontent

    based onanalyzinghistoricalrecord ID or query_id generatingwithpushnotificationformatconsistent Markdown report。

    Args:
        record_id: analyzinghistoricalrecordprimary key ID（integer）or query_id（string）
        db_manager: databasemanagerdependency

    Returns:
        MarkdownReportResponse: Markdown formatFull Report

    Raises:
        HTTPException: 404 - reportnot found
        HTTPException: 500 - report generationfailed（servicehandler internalerror）
    """
    service = HistoryService(db_manager)

    try:
        markdown_content = service.get_markdown_report(record_id)
    except MarkdownReportGenerationError as e:
        logger.error(f"Markdown report generation failed for {record_id}: {e.message}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "generation_failed",
                "message": f"generating Markdown reportfailed: {e.message}"
            }
        )
    except Exception as e:
        logger.error(f"get Markdown reportfailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"get Markdown reportfailed: {str(e)}"
            }
        )

    if markdown_content is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "not_found",
                "message": f"not found id/query_id={record_id} analyzingrecord"
            }
        )

    return MarkdownReportResponse(content=markdown_content)
