# -*- coding: utf-8 -*-
"""
===================================
healthcheckAPI/interface
===================================

Responsibilities:
1. provide /api/v1/health healthcheckAPI/interface
2. forload balancinghandlerandmonitorsystem
"""

from datetime import datetime

from fastapi import APIRouter

from api.v1.schemas.common import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    healthcheckAPI/interface
    
    forload balancinghandlerormonitorsystemcheckservicestatus
    
    Returns:
        HealthResponse: packageincludeservicestatusandtimestamp
    """
    return HealthResponse(
        status="ok",
        timestamp=datetime.now().isoformat()
    )
