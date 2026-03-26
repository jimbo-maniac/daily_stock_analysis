# -*- coding: utf-8 -*-
"""
===================================
analyzingrelatedmodel
===================================

Responsibilities:
1. defineanalyzingrequestandresponsemodel
2. definetask statusmodel
3. defineasynchronoustaskqueuerelatedmodel
"""

from typing import Optional, List, Any
from enum import Enum

from pydantic import BaseModel, Field
from src.utils.analysis_metadata import SELECTION_SOURCE_PATTERN


class TaskStatusEnum(str, Enum):
    """task statusenum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class AnalyzeRequest(BaseModel):
    """Analysis request parameters"""
    
    stock_code: Optional[str] = Field(
        None, 
        description="singlestock code", 
        example="600519"
    )
    stock_codes: Optional[List[str]] = Field(
        None, 
        description="multipleonlystock code（with stock_code twoselectone）",
        example=["600519", "000858"]
    )
    report_type: str = Field(
        "detailed",
        description="report type：simple(simplified) / detailed(complete) / full(complete) / brief(concise)",
        pattern="^(simple|detailed|full|brief)$",
    )
    force_refresh: bool = Field(
        False,
        description="whethermandatoryrefresh（ignorecache）"
    )
    async_mode: bool = Field(
        False,
        description="whetheruseasynchronousmode"
    )
    stock_name: Optional[str] = Field(
        None,
        description="userselectinstockname（automaticcompletewhenprovide）",
        example="Kweichow Moutai"
    )
    original_query: Optional[str] = Field(
        None,
        description="userrawinput（e.g.Maotai、gzmt、600519）",
        example="Maotai"
    )
    selection_source: Optional[str] = Field(
        None,
        description="stockselect source：manual(manualinput) | autocomplete(automaticcomplete) | import(import) | image(imageidentify)",
        pattern=SELECTION_SOURCE_PATTERN,
        example="autocomplete"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "stock_code": "600519",
                "report_type": "detailed",
                "force_refresh": False,
                "async_mode": False,
                "stock_name": "Kweichow Moutai",
                "original_query": "Maotai",
                "selection_source": "autocomplete"
            }
        }


class AnalysisResultResponse(BaseModel):
    """analysis resultresponsemodel"""
    
    query_id: str = Field(..., description="analyzingrecordunique identifier")
    stock_code: str = Field(..., description="stock code")
    stock_name: Optional[str] = Field(None, description="stockname")
    report: Optional[Any] = Field(None, description="analysis report")
    created_at: str = Field(..., description="creation time")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query_id": "abc123def456",
                "stock_code": "600519",
                "stock_name": "Kweichow Moutai",
                "report": {
                    "summary": {
                        "sentiment_score": 75,
                        "operation_advice": "hold"
                    }
                },
                "created_at": "2024-01-01T12:00:00"
            }
        }


class TaskAccepted(BaseModel):
    """asynchronoustaskacceptresponse"""
    
    task_id: str = Field(..., description="task ID，forqueryingstatus")
    status: str = Field(
        ..., 
        description="task status",
        pattern="^(pending|processing)$"
    )
    message: Optional[str] = Field(None, description="prompt message")
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "task_abc123",
                "status": "pending",
                "message": "Analysis task accepted"
            }
        }


class BatchTaskAcceptedItem(BaseModel):
    """batchasynchronoustaskinsinglesuccessfulsubmititem。"""

    task_id: str = Field(..., description="task ID，forqueryingstatus")
    stock_code: str = Field(..., description="stock code")
    status: str = Field(
        ...,
        description="task status",
        pattern="^(pending|processing)$"
    )
    message: Optional[str] = Field(None, description="prompt message")

    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "task_abc123",
                "stock_code": "600519",
                "status": "pending",
                "message": "analyzingtaskaddedqueue: 600519"
            }
        }


class BatchDuplicateTaskItem(BaseModel):
    """batchasynchronoustaskinduplicatesubmititem。"""

    stock_code: str = Field(..., description="stock code")
    existing_task_id: str = Field(..., description="already existstask ID")
    message: str = Field(..., description="error message")

    class Config:
        json_schema_extra = {
            "example": {
                "stock_code": "600519",
                "existing_task_id": "task_existing_123",
                "message": "stock 600519 currentlyanalyzingin (task_id: task_existing_123)"
            }
        }


class BatchTaskAcceptedResponse(BaseModel):
    """batchasynchronoustaskacceptresponse。"""

    accepted: List[BatchTaskAcceptedItem] = Field(default_factory=list, description="successfulsubmittasklist")
    duplicates: List[BatchDuplicateTaskItem] = Field(default_factory=list, description="duplicateandskiptasklist")
    message: str = Field(..., description="summaryinfo")

    class Config:
        json_schema_extra = {
            "example": {
                "accepted": [
                    {
                        "task_id": "task_abc123",
                        "stock_code": "600519",
                        "status": "pending",
                        "message": "analyzingtaskaddedqueue: 600519"
                    }
                ],
                "duplicates": [
                    {
                        "stock_code": "000858",
                        "existing_task_id": "task_existing_456",
                        "message": "stock 000858 currentlyanalyzingin (task_id: task_existing_456)"
                    }
                ],
                "message": "alreadysubmit 1 counttask，1 countduplicateskip"
            }
        }


class TaskStatus(BaseModel):
    """Task status model"""
    
    task_id: str = Field(..., description="task ID")
    status: str = Field(
        ..., 
        description="task status",
        pattern="^(pending|processing|completed|failed)$"
    )
    progress: Optional[int] = Field(
        None, 
        description="progresspercentage (0-100)",
        ge=0,
        le=100
    )
    result: Optional[AnalysisResultResponse] = Field(
        None, 
        description="analysis result（only in completed exists when）"
    )
    error: Optional[str] = Field(
        None, 
        description="error message（only in failed exists when）"
    )
    stock_name: Optional[str] = Field(None, description="stockname")
    original_query: Optional[str] = Field(None, description="userrawinput")
    selection_source: Optional[str] = Field(
        None,
        description="select source",
        pattern=SELECTION_SOURCE_PATTERN,
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "task_abc123",
                "status": "completed",
                "progress": 100,
                "result": None,
                "error": None,
                "stock_name": "Kweichow Moutai",
                "original_query": "Maotai",
                "selection_source": "autocomplete"
            }
        }


class TaskInfo(BaseModel):
    """
    Task details model

    Used for task list and SSE event delivery
    """
    
    task_id: str = Field(..., description="task ID")
    stock_code: str = Field(..., description="stock code")
    stock_name: Optional[str] = Field(None, description="stockname")
    status: TaskStatusEnum = Field(..., description="task status")
    progress: int = Field(0, description="progresspercentage (0-100)", ge=0, le=100)
    message: Optional[str] = Field(None, description="statusmessage")
    report_type: str = Field("detailed", description="report type")
    created_at: str = Field(..., description="creation time")
    started_at: Optional[str] = Field(None, description="startingexecution time")
    completed_at: Optional[str] = Field(None, description="completedtime")
    error: Optional[str] = Field(None, description="error message（only in failed exists when）")
    original_query: Optional[str] = Field(None, description="userrawinput")
    selection_source: Optional[str] = Field(
        None,
        description="select source",
        pattern=SELECTION_SOURCE_PATTERN,
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "abc123def456",
                "stock_code": "600519",
                "stock_name": "Kweichow Moutai",
                "status": "processing",
                "progress": 50,
                "message": "currentlyanalyzingin...",
                "report_type": "detailed",
                "created_at": "2026-02-05T10:30:00",
                "started_at": "2026-02-05T10:30:01",
                "completed_at": None,
                "error": None,
                "original_query": "Maotai",
                "selection_source": "autocomplete"
            }
        }


class TaskListResponse(BaseModel):
    """tasklistresponsemodel"""
    
    total: int = Field(..., description="tasktotal count")
    pending: int = Field(..., description="waitingintaskcount")
    processing: int = Field(..., description="processingintaskcount")
    tasks: List[TaskInfo] = Field(..., description="tasklist")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total": 3,
                "pending": 1,
                "processing": 2,
                "tasks": []
            }
        }


class DuplicateTaskErrorResponse(BaseModel):
    """duplicatetaskerrorresponsemodel"""
    
    error: str = Field("duplicate_task", description="errortype")
    message: str = Field(..., description="error message")
    stock_code: str = Field(..., description="stock code")
    existing_task_id: str = Field(..., description="already existstask ID")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "duplicate_task",
                "message": "stock 600519 currentlyanalyzingin",
                "stock_code": "600519",
                "existing_task_id": "abc123def456"
            }
        }
