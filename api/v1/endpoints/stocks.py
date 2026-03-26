# -*- coding: utf-8 -*-
"""
===================================
stockdataAPI/interface
===================================

Responsibilities:
1. POST /api/v1/stocks/extract-from-image fromimageextractstock code
2. POST /api/v1/stocks/parse-import parsing CSV/Excel/clipboard
3. GET /api/v1/stocks/{code}/quote realtimequote/market dataAPI/interface
4. GET /api/v1/stocks/{code}/history historicalquote/market dataAPI/interface
"""

import logging
from typing import Optional

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile

from api.v1.schemas.stocks import (
    ExtractFromImageResponse,
    ExtractItem,
    KLineData,
    StockHistoryResponse,
    StockQuote,
)
from api.v1.schemas.common import ErrorResponse
from src.services.image_stock_extractor import (
    ALLOWED_MIME,
    MAX_SIZE_BYTES,
    extract_stock_codes_from_image,
)
from src.services.import_parser import (
    MAX_FILE_BYTES,
    parse_import_from_bytes,
    parse_import_from_text,
)
from src.services.stock_service import StockService

logger = logging.getLogger(__name__)

router = APIRouter()

# mustin /{stock_code} routebeforedefine
ALLOWED_MIME_STR = ", ".join(ALLOWED_MIME)


@router.post(
    "/extract-from-image",
    response_model=ExtractFromImageResponse,
    responses={
        200: {"description": "extractstock code"},
        400: {"description": "imageinvalid", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="fromimageextractstock code",
    description="uploadingscreenshot/image，via Vision LLM extractstock code。support JPEG、PNG、WebP、GIF，max 5MB。",
)
def extract_from_image(
    file: Optional[UploadFile] = File(None, description="imagefile（formfieldname file）"),
    include_raw: bool = Query(False, description="whether inresultinpackageincluderaw LLM response"),
) -> ExtractFromImageResponse:
    """
    fromuploadingimageextract fromstock code（use Vision LLM）。

    formfieldplease use file uploadingimage。priority：Gemini / Anthropic / OpenAI（first available）。
    """
    if not file or not file.filename:
        raise HTTPException(
            status_code=400,
            detail={"error": "bad_request", "message": "not providedfile，please useformfield file uploadingimage"},
        )

    content_type = (file.content_type or "").split(";")[0].strip().lower()
    if content_type not in ALLOWED_MIME:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_type",
                "message": f"unsupportedtype: {content_type}。allow: {ALLOWED_MIME_STR}",
            },
        )

    try:
        # firstreadinglimitfixedsize，againcheckwhetherstillhasremaining（languagedefinitionclear：exceedthenreject）
        data = file.file.read(MAX_SIZE_BYTES)
        if file.file.read(1):
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "file_too_large",
                    "message": f"imageexceed {MAX_SIZE_BYTES // (1024 * 1024)}MB constraint",
                },
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"readinguploadingfilefailed: {e}")
        raise HTTPException(
            status_code=400,
            detail={"error": "read_failed", "message": "readinguploadingfilefailed"},
        )

    try:
        items, raw_text = extract_stock_codes_from_image(data, content_type)
        extract_items = [
            ExtractItem(code=code, name=name, confidence=conf) for code, name, conf in items
        ]
        codes = [i.code for i in extract_items]
        return ExtractFromImageResponse(
            codes=codes,
            items=extract_items,
            raw_text=raw_text if include_raw else None,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": "extract_failed", "message": str(e)})
    except Exception as e:
        logger.error(f"imageextractfailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": "imageextractfailed"},
        )


@router.post(
    "/parse-import",
    response_model=ExtractFromImageResponse,
    responses={
        200: {"description": "parsingresult"},
        400: {"description": "not provideddataorparse failed", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="parsing CSV/Excel/clipboard",
    description="uploading CSV/Excel fileorpastetext，automaticparsingstock code。fileupper limit 2MB，textupper limit 100KB。",
)
async def parse_import(request: Request) -> ExtractFromImageResponse:
    """
    parsing CSV/Excel fileorclipboardtext。

    - multipart/form-data + file: uploadingfile
    - application/json + {"text": "..."}: pastetext
    - prefer to use file，ifsimultaneouslyprovidethenignore text
    """
    content_type = (request.headers.get("content-type") or "").lower()

    if "application/json" in content_type:
        try:
            body = await request.json()
        except Exception as e:
            logger.warning("[parse_import] JSON parse failed: %s", e)
            raise HTTPException(
                status_code=400,
                detail={"error": "invalid_json", "message": f"JSON parse failed: {e}"},
            )
        text = body.get("text") if isinstance(body, dict) else None
        if not text or not isinstance(text, str):
            raise HTTPException(
                status_code=400,
                detail={"error": "bad_request", "message": "not provided text，please use {\"text\": \"...\"}"},
            )
        try:
            items = parse_import_from_text(text)
        except ValueError as e:
            text_bytes = len(text.encode("utf-8"))
            logger.warning(
                "[parse_import] parse_import_from_text failed: text_bytes=%d, error=%s",
                text_bytes,
                e,
            )
            raise HTTPException(status_code=400, detail={"error": "parse_failed", "message": str(e)})
    elif "multipart" in content_type:
        form = await request.form()
        file = form.get("file")
        if not file or not hasattr(file, "read"):
            raise HTTPException(
                status_code=400,
                detail={"error": "bad_request", "message": "not providedfile，please useformfield file"},
            )
        file_size = getattr(file, "size", None)
        if isinstance(file_size, int) and file_size > MAX_FILE_BYTES:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "file_too_large",
                    "message": f"fileexceed {MAX_FILE_BYTES // (1024 * 1024)}MB constraint",
                },
            )
        try:
            data = file.file.read(MAX_FILE_BYTES)
            if file.file.read(1):
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "file_too_large",
                        "message": f"fileexceed {MAX_FILE_BYTES // (1024 * 1024)}MB constraint",
                    },
                )
        except HTTPException:
            raise
        except Exception as e:
            filename = getattr(file, "filename", None) or ""
            size = getattr(file, "size", None)
            logger.warning(
                "[parse_import] file read failed: filename=%r, size=%s, error=%s",
                filename,
                size,
                e,
            )
            raise HTTPException(
                status_code=400,
                detail={"error": "read_failed", "message": "readingfilefailed"},
            )
        filename = getattr(file, "filename", None) or ""
        try:
            items = parse_import_from_bytes(data, filename=filename)
        except ValueError as e:
            ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            logger.warning(
                "[parse_import] parse_import_from_bytes failed: filename=%r, ext=%r, bytes=%d, error=%s",
                filename,
                ext,
                len(data),
                e,
            )
            raise HTTPException(status_code=400, detail={"error": "parse_failed", "message": str(e)})
    else:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "bad_request",
                "message": "please use multipart/form-data uploadingfile，or application/json submit {\"text\": \"...\"}",
            },
        )

    extract_items = [
        ExtractItem(code=code, name=name, confidence=conf)
        for code, name, conf in items
    ]
    codes = list(dict.fromkeys(i.code for i in extract_items if i.code))
    return ExtractFromImageResponse(codes=codes, items=extract_items, raw_text=None)


@router.get(
    "/{stock_code}/quote",
    response_model=StockQuote,
    responses={
        200: {"description": "quote/market datadata"},
        404: {"description": "stocknot found", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="getstockrealtimequote/market data",
    description="get specifiedstocklatestquote/market datadata"
)
def get_stock_quote(stock_code: str) -> StockQuote:
    """
    getstockrealtimequote/market data
    
    get specifiedstocklatestquote/market datadata
    
    Args:
        stock_code: stock code（e.g. 600519、00700、AAPL）
        
    Returns:
        StockQuote: realtimequote/market datadata
        
    Raises:
        HTTPException: 404 - stocknot found
    """
    try:
        service = StockService()
        
        # use def instead of async def，FastAPI automatically inthreadexecute in pool
        result = service.get_realtime_quote(stock_code)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "not_found",
                    "message": f"not foundstock {stock_code} quote/market datadata"
                }
            )
        
        return StockQuote(
            stock_code=result.get("stock_code", stock_code),
            stock_name=result.get("stock_name"),
            current_price=result.get("current_price", 0.0),
            change=result.get("change"),
            change_percent=result.get("change_percent"),
            open=result.get("open"),
            high=result.get("high"),
            low=result.get("low"),
            prev_close=result.get("prev_close"),
            volume=result.get("volume"),
            amount=result.get("amount"),
            update_time=result.get("update_time")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get realtimequote/market datafailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"get realtimequote/market datafailed: {str(e)}"
            }
        )


@router.get(
    "/{stock_code}/history",
    response_model=StockHistoryResponse,
    responses={
        200: {"description": "historicalquote/market datadata"},
        422: {"description": "unsupportedperiodparameter", "model": ErrorResponse},
        500: {"description": "servicehandlererror", "model": ErrorResponse},
    },
    summary="getstockhistoricalquote/market data",
    description="get specifiedstockhistorical K linedata"
)
def get_stock_history(
    stock_code: str,
    period: str = Query("daily", description="K line period", pattern="^(daily|weekly|monthly)$"),
    days: int = Query(30, ge=1, le=365, description="get days count")
) -> StockHistoryResponse:
    """
    getstockhistoricalquote/market data
    
    get specifiedstockhistorical K linedata
    
    Args:
        stock_code: stock code
        period: K line period (daily/weekly/monthly)
        days: get days count
        
    Returns:
        StockHistoryResponse: historicalquote/market datadata
    """
    try:
        service = StockService()
        
        # use def instead of async def，FastAPI automatically inthreadexecute in pool
        result = service.get_history_data(
            stock_code=stock_code,
            period=period,
            days=days
        )
        
        # convertingasresponsemodel
        data = [
            KLineData(
                date=item.get("date"),
                open=item.get("open"),
                high=item.get("high"),
                low=item.get("low"),
                close=item.get("close"),
                volume=item.get("volume"),
                amount=item.get("amount"),
                change_percent=item.get("change_percent")
            )
            for item in result.get("data", [])
        ]
        
        return StockHistoryResponse(
            stock_code=stock_code,
            stock_name=result.get("stock_name"),
            period=period,
            data=data
        )
    
    except ValueError as e:
        # period parameterunsupportederror（e.g. weekly/monthly）
        raise HTTPException(
            status_code=422,
            detail={
                "error": "unsupported_period",
                "message": str(e)
            }
        )
    except Exception as e:
        logger.error(f"get historicalquote/market datafailed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "internal_error",
                "message": f"get historicalquote/market datafailed: {str(e)}"
            }
        )
