# -*- coding: utf-8 -*-
"""
===================================
stockdatarelatedmodel
===================================

Responsibilities:
1. definestockrealtimequote/market datamodel
2. definehistorical K linedatamodel
"""

from typing import Optional, List

from pydantic import BaseModel, Field


class StockQuote(BaseModel):
    """stockrealtimequote/market data"""
    
    stock_code: str = Field(..., description="stock code")
    stock_name: Optional[str] = Field(None, description="stockname")
    current_price: float = Field(..., description="currentprice")
    change: Optional[float] = Field(None, description="price change amount")
    change_percent: Optional[float] = Field(None, description="price change percentage (%)")
    open: Optional[float] = Field(None, description="opening price")
    high: Optional[float] = Field(None, description="highest price")
    low: Optional[float] = Field(None, description="lowest price")
    prev_close: Optional[float] = Field(None, description="yesterday closeprice")
    volume: Optional[float] = Field(None, description="trading volume（stocks）")
    amount: Optional[float] = Field(None, description="trading amount（yuan）")
    update_time: Optional[str] = Field(None, description="update time")
    
    class Config:
        json_schema_extra = {
            "example": {
                "stock_code": "600519",
                "stock_name": "Kweichow Moutai",
                "current_price": 1800.00,
                "change": 15.00,
                "change_percent": 0.84,
                "open": 1785.00,
                "high": 1810.00,
                "low": 1780.00,
                "prev_close": 1785.00,
                "volume": 10000000,
                "amount": 18000000000,
                "update_time": "2024-01-01T15:00:00"
            }
        }


class KLineData(BaseModel):
    """K linedatapoint"""
    
    date: str = Field(..., description="date")
    open: float = Field(..., description="opening price")
    high: float = Field(..., description="highest price")
    low: float = Field(..., description="lowest price")
    close: float = Field(..., description="closing price")
    volume: Optional[float] = Field(None, description="trading volume")
    amount: Optional[float] = Field(None, description="trading amount")
    change_percent: Optional[float] = Field(None, description="price change percentage (%)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "date": "2024-01-01",
                "open": 1785.00,
                "high": 1810.00,
                "low": 1780.00,
                "close": 1800.00,
                "volume": 10000000,
                "amount": 18000000000,
                "change_percent": 0.84
            }
        }


class ExtractItem(BaseModel):
    """single entryextractresult（code、name、confidence）"""

    code: Optional[str] = Field(None, description="stock code，None indicatesparse failed")
    name: Optional[str] = Field(None, description="stockname（e.g.has）")
    confidence: str = Field("medium", description="confidence：high/medium/low")


class ExtractFromImageResponse(BaseModel):
    """imagestock codeextractresponse"""

    codes: List[str] = Field(..., description="extractstock code（alreadydeduplicate，toaftercompatible）")
    items: List[ExtractItem] = Field(default_factory=list, description="extractresultcleardetail（code+name+confidence）")
    raw_text: Optional[str] = Field(None, description="raw LLM response（debuguse）")


class StockHistoryResponse(BaseModel):
    """stockhistoricalquote/market dataresponse"""
    
    stock_code: str = Field(..., description="stock code")
    stock_name: Optional[str] = Field(None, description="stockname")
    period: str = Field(..., description="K line period")
    data: List[KLineData] = Field(default_factory=list, description="K linedatalist")
    
    class Config:
        json_schema_extra = {
            "example": {
                "stock_code": "600519",
                "stock_name": "Kweichow Moutai",
                "period": "daily",
                "data": []
            }
        }
