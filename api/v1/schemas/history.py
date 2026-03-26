# -*- coding: utf-8 -*-
"""
===================================
historicalrecordrelatedmodel
===================================

Responsibilities:
1. definehistoricalrecordlistanddetailsmodel
2. defineanalysis reportcompletemodel
"""

from typing import Optional, List, Any

from pydantic import BaseModel, ConfigDict, Field


class HistoryItem(BaseModel):
    """historicalrecordsummary（listdisplayuse）"""

    id: Optional[int] = Field(None, description="analyzinghistoricalrecordprimary key ID")
    query_id: str = Field(..., description="analyzingrecordassociation query_id（batchanalyzingwhenduplicate）")
    stock_code: str = Field(..., description="stock code")
    stock_name: Optional[str] = Field(None, description="stockname")
    report_type: Optional[str] = Field(None, description="report type")
    sentiment_score: Optional[int] = Field(
        None, 
        description="sentimentscore (0-100)",
        ge=0,
        le=100
    )
    operation_advice: Optional[str] = Field(None, description="operationrecommended")
    created_at: Optional[str] = Field(None, description="creation time")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": 1234,
                "query_id": "abc123",
                "stock_code": "600519",
                "stock_name": "Kweichow Moutai",
                "report_type": "detailed",
                "sentiment_score": 75,
                "operation_advice": "hold",
                "created_at": "2024-01-01T12:00:00"
            }
        }


class HistoryListResponse(BaseModel):
    """historicalrecordlistresponse"""
    
    total: int = Field(..., description="totalrecordcount")
    page: int = Field(..., description="currentpage number")
    limit: int = Field(..., description="items per page")
    items: List[HistoryItem] = Field(default_factory=list, description="recordlist")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total": 100,
                "page": 1,
                "limit": 20,
                "items": []
            }
        }


class DeleteHistoryRequest(BaseModel):
    """deletinghistoricalrecordrequest"""

    record_ids: List[int] = Field(default_factory=list, description="needdeletinghistoricalrecordprimary key ID list")


class DeleteHistoryResponse(BaseModel):
    """deletinghistoricalrecordresponse"""

    deleted: int = Field(..., description="actualdeletinghistoricalrecordquantity")


class NewsIntelItem(BaseModel):
    """newsintelligenceitemsitem"""

    title: str = Field(..., description="newstitle")
    snippet: str = Field("", description="newssummary（at most200character）")
    url: str = Field(..., description="newslink")

    class Config:
        json_schema_extra = {
            "example": {
                "title": "companypublishperformanceflash report，revenueyear-on-year growth 20%",
                "snippet": "companyannouncementdisplay，quarterrevenueyear-on-year growth 20%...",
                "url": "https://example.com/news/123"
            }
        }


class NewsIntelResponse(BaseModel):
    """newsintelligenceresponse"""

    total: int = Field(..., description="newscount")
    items: List[NewsIntelItem] = Field(default_factory=list, description="newslist")

    class Config:
        json_schema_extra = {
            "example": {
                "total": 2,
                "items": []
            }
        }


class ReportMeta(BaseModel):
    """reportyuaninfo"""

    model_config = ConfigDict(protected_namespaces=("model_validate", "model_dump"))

    id: Optional[int] = Field(None, description="analyzinghistoricalrecordprimary key ID（onlyhistoricalreporthasthisfield）")
    query_id: str = Field(..., description="analyzingrecordassociation query_id（batchanalyzingwhenduplicate）")
    stock_code: str = Field(..., description="stock code")
    stock_name: Optional[str] = Field(None, description="stockname")
    report_type: Optional[str] = Field(None, description="report type")
    report_language: Optional[str] = Field(None, description="reportoutputlanguage（zh/en）")
    created_at: Optional[str] = Field(None, description="creation time")
    current_price: Optional[float] = Field(None, description="analyzingwhenstock price")
    change_pct: Optional[float] = Field(None, description="analyzingwhenprice change percentage(%)")
    model_used: Optional[str] = Field(None, description="analyzinguse LLM model")


class ReportSummary(BaseModel):
    """reportoverviewzone"""
    
    analysis_summary: Optional[str] = Field(None, description="keyconclusion")
    operation_advice: Optional[str] = Field(None, description="operationrecommended")
    trend_prediction: Optional[str] = Field(None, description="trendprediction")
    sentiment_score: Optional[int] = Field(
        None, 
        description="sentimentscore (0-100)",
        ge=0,
        le=100
    )
    sentiment_label: Optional[str] = Field(None, description="sentimentlabel")


class ReportStrategy(BaseModel):
    """strategypointdigitzone"""
    
    ideal_buy: Optional[str] = Field(None, description="reasonthinkbuyprice")
    secondary_buy: Optional[str] = Field(None, description="thetwobuyprice")
    stop_loss: Optional[str] = Field(None, description="stop lossprice")
    take_profit: Optional[str] = Field(None, description="take profitprice")


class ReportDetails(BaseModel):
    """report detailszone"""
    
    news_content: Optional[str] = Field(None, description="newssummary")
    raw_result: Optional[Any] = Field(None, description="rawanalysis result（JSON）")
    context_snapshot: Optional[Any] = Field(None, description="analyzingwhencontextsnapshot（JSON）")
    financial_report: Optional[Any] = Field(None, description="structure-izefinancial reportsummary（from fundamental_context）")
    dividend_metrics: Optional[Any] = Field(None, description="structure-izedividendindicator（include TTM caliber）")


class AnalysisReport(BaseModel):
    """completeanalysis report"""

    meta: ReportMeta = Field(..., description="yuaninfo")
    summary: ReportSummary = Field(..., description="overviewzone")
    strategy: Optional[ReportStrategy] = Field(None, description="strategypointdigitzone")
    details: Optional[ReportDetails] = Field(None, description="detailszone")

    class Config:
        json_schema_extra = {
            "example": {
                "meta": {
                    "query_id": "abc123",
                    "stock_code": "600519",
                    "stock_name": "Kweichow Moutai",
                    "report_type": "detailed",
                    "report_language": "zh",
                    "created_at": "2024-01-01T12:00:00"
                },
                "summary": {
                    "analysis_summary": "technical aspect improving，recommendedhold",
                    "operation_advice": "hold",
                    "trend_prediction": "bullish",
                    "sentiment_score": 75,
                    "sentiment_label": "optimistic"
                },
                "strategy": {
                    "ideal_buy": "1800.00",
                    "secondary_buy": "1750.00",
                    "stop_loss": "1700.00",
                    "take_profit": "2000.00"
                },
                "details": None
            }
        }


class MarkdownReportResponse(BaseModel):
    """Markdown formatreportresponse"""

    content: str = Field(..., description="Markdown formatFull Reportcontent")

    class Config:
        json_schema_extra = {
            "example": {
                "content": "# 📊 Kweichow Moutai (600519) analysis report\n\n> analyzingdate：**2024-01-01**\n\n..."
            }
        }
