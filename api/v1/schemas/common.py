# -*- coding: utf-8 -*-
"""
===================================
genericresponsemodel
===================================

Responsibilities:
1. definegenericresponsemodel（HealthResponse, ErrorResponse etc）
2. provide unifiedresponseformat
"""

from typing import Optional, Any

from pydantic import BaseModel, Field


class RootResponse(BaseModel):
    """API rootrouteresponse"""
    
    message: str = Field(..., description="API runningstatusmessage", example="Daily Stock Analysis API is running")
    version: Optional[str] = Field(None, description="API version", example="1.0.0")
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": "Daily Stock Analysis API is running",
                "version": "1.0.0"
            }
        }


class HealthResponse(BaseModel):
    """healthcheckresponse"""
    
    status: str = Field(..., description="servicestatus", example="ok")
    timestamp: Optional[str] = Field(None, description="timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "ok",
                "timestamp": "2024-01-01T12:00:00"
            }
        }


class ErrorResponse(BaseModel):
    """errorresponse"""
    
    error: str = Field(..., description="errortype", example="validation_error")
    message: str = Field(..., description="errordetails", example="requestparametererror")
    detail: Optional[Any] = Field(None, description="attacherror message")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "not_found",
                "message": "capitalsourcenot found",
                "detail": None
            }
        }


class SuccessResponse(BaseModel):
    """genericsuccessfulresponse"""
    
    success: bool = Field(True, description="whethersuccessful")
    message: Optional[str] = Field(None, description="successfulmessage")
    data: Optional[Any] = Field(None, description="responsedata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "operation successful",
                "data": None
            }
        }
