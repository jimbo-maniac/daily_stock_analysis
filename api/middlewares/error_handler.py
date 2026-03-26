# -*- coding: utf-8 -*-
"""
===================================
globalabnormalprocessingmiddleware
===================================

Responsibilities:
1. capturenotprocessingabnormal
2. unifiederrorresponseformat
3. recorderrorlog
"""

import logging
import traceback
from typing import Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """
    globalabnormalprocessingmiddleware
    
    captureallnotprocessingabnormal，returnunifiedformaterrorresponse
    """
    
    async def dispatch(
        self, 
        request: Request, 
        call_next: Callable
    ) -> Response:
        """
        processingrequest，captureabnormal
        
        Args:
            request: requestobject
            call_next: belowonecounthandler
            
        Returns:
            Response: responseobject
        """
        try:
            response = await call_next(request)
            return response
            
        except Exception as e:
            # recorderrorlog
            logger.error(
                f"notprocessingabnormal: {e}\n"
                f"requestpath: {request.url.path}\n"
                f"request method: {request.method}\n"
                f"heapstack: {traceback.format_exc()}"
            )
            
            # returnunifiedformaterrorresponse
            return JSONResponse(
                status_code=500,
                content={
                    "error": "internal_error",
                    "message": "servicehandler internalerror，pleaseslightlyafterretry",
                    "detail": str(e) if logger.isEnabledFor(logging.DEBUG) else None
                }
            )


def add_error_handlers(app) -> None:
    """
    addglobalabnormalhandler
    
    as FastAPI applyaddeachclassabnormalhandler
    
    Args:
        app: FastAPI applyinstance
    """
    from fastapi import HTTPException
    from fastapi.exceptions import RequestValidationError
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """processing HTTP abnormal"""
        # if detail alreadythroughis ErrorResponse format dict，directlyuse
        if isinstance(exc.detail, dict) and "error" in exc.detail and "message" in exc.detail:
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.detail
            )
        # otherwisewill detail packageinstallbecome ErrorResponse format
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": "http_error",
                "message": str(exc.detail) if exc.detail else "HTTP Error",
                "detail": None
            }
        )
    
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """processingrequestverificationabnormal"""
        return JSONResponse(
            status_code=422,
            content={
                "error": "validation_error",
                "message": "requestparameterverificationfailed",
                "detail": exc.errors()
            }
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """processinggenericabnormal"""
        logger.error(
            f"notprocessingabnormal: {exc}\n"
            f"requestpath: {request.url.path}\n"
            f"heapstack: {traceback.format_exc()}"
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "message": "servicehandler internalerror",
                "detail": None
            }
        )
