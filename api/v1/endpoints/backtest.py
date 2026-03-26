# -*- coding: utf-8 -*-
"""Backtest endpoints."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_database_manager
from api.v1.schemas.backtest import (
    BacktestRunRequest,
    BacktestRunResponse,
    BacktestResultItem,
    BacktestResultsResponse,
    PerformanceMetrics,
)
from api.v1.schemas.common import ErrorResponse
from src.services.backtest_service import BacktestService
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/run",
    response_model=BacktestRunResponse,
    responses={
        200: {"description": "backtest executioncompleted"},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="triggerbacktest",
    description="tohistoricalanalyzingrecordproceedbacktestevaluation，andwriting backtest_results/backtest_summaries",
)
def run_backtest(
    request: BacktestRunRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> BacktestRunResponse:
    try:
        service = BacktestService(db_manager)
        stats = service.run_backtest(
            code=request.code,
            force=request.force,
            eval_window_days=request.eval_window_days,
            min_age_days=request.min_age_days,
            limit=request.limit,
        )
        return BacktestRunResponse(**stats)
    except Exception as exc:
        logger.error(f"backtest executionfailed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"backtest executionfailed: {str(exc)}"},
        )


@router.get(
    "/results",
    response_model=BacktestResultsResponse,
    responses={
        200: {"description": "backtest resultlist"},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="getbacktest result",
    description="paginationgetbacktest result，support bystock codefiltering",
)
def get_backtest_results(
    code: Optional[str] = Query(None, description="stock codefilter"),
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="evaluationwindowfiltering"),
    page: int = Query(1, ge=1, description="page number"),
    limit: int = Query(20, ge=1, le=200, description="items per page"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> BacktestResultsResponse:
    try:
        service = BacktestService(db_manager)
        data = service.get_recent_evaluations(code=code, eval_window_days=eval_window_days, limit=limit, page=page)
        items = [BacktestResultItem(**item) for item in data.get("items", [])]
        return BacktestResultsResponse(
            total=int(data.get("total", 0)),
            page=page,
            limit=limit,
            items=items,
        )
    except Exception as exc:
        logger.error(f"queryingbacktest resultfailed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"queryingbacktest resultfailed: {str(exc)}"},
        )


@router.get(
    "/performance",
    response_model=PerformanceMetrics,
    responses={
        200: {"description": "overallbacktestperformance"},
        404: {"description": "nobacktestsummary", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="getoverallbacktestperformance",
)
def get_overall_performance(
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="evaluationwindowfiltering"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> PerformanceMetrics:
    try:
        service = BacktestService(db_manager)
        summary = service.get_summary(scope="overall", code=None, eval_window_days=eval_window_days)
        if summary is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": "not foundoverallbacktestsummary"},
            )
        return PerformanceMetrics(**summary)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"queryingoverallperformancefailed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"queryingoverallperformancefailed: {str(exc)}"},
        )


@router.get(
    "/performance/{code}",
    response_model=PerformanceMetrics,
    responses={
        200: {"description": "single stockbacktestperformance"},
        404: {"description": "nobacktestsummary", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="getsingle stockbacktestperformance",
)
def get_stock_performance(
    code: str,
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="evaluationwindowfiltering"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> PerformanceMetrics:
    try:
        service = BacktestService(db_manager)
        summary = service.get_summary(scope="stock", code=code, eval_window_days=eval_window_days)
        if summary is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": f"not found {code} backtestsummary"},
            )
        return PerformanceMetrics(**summary)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"queryingsingle stockperformancefailed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"queryingsingle stockperformancefailed: {str(exc)}"},
        )

