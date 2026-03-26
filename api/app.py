# -*- coding: utf-8 -*-
"""
===================================
FastAPI applyfactorymodule
===================================

Responsibilities:
1. creatingandconfiguration FastAPI applyinstance
2. configuration CORS middleware
3. registerrouteandabnormalhandler
4. hostingfrontendstaticfile（productionmode）

usage：
    from api.app import create_app
    app = create_app()
"""

import mimetypes
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from api.v1 import api_v1_router
from api.middlewares.auth import add_auth_middleware
from api.middlewares.error_handler import add_error_handlers
from api.v1.schemas.common import HealthResponse
from src.services.system_config_service import SystemConfigService


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Initialize and release shared services for the app lifecycle."""
    app.state.system_config_service = SystemConfigService()
    try:
        yield
    finally:
        if hasattr(app.state, "system_config_service"):
            delattr(app.state, "system_config_service")


def create_app(static_dir: Optional[Path] = None) -> FastAPI:
    """
    creatingandconfiguration FastAPI applyinstance
    
    Args:
        static_dir: staticfiledirectorypath（optional，defaultasitemitemrootdirectorybelow static）
        
    Returns:
        configurationcompleted FastAPI applyinstance
    """
    # defaultstaticfiledirectory
    if static_dir is None:
        static_dir = Path(__file__).parent.parent / "static"
    
    # creating FastAPI instance
    app = FastAPI(
        title="Daily Stock Analysis API",
        description=(
            "A-share/HK stock/US stockwatchlist stocksintelligentanalyzingsystem API\n\n"
            "## featuremodule\n"
            "- stockanalyzing：trigger AI intelligentanalyzing\n"
            "- historicalrecord：queryinghistoricalanalysis report\n"
            "- stockdata：getquote/market datadata\n\n"
            "## authenticationmethod\n"
            "supportoptionalrunningwhenauthentication（via WebUI settingspageenabled/close）"
        ),
        version="1.0.0",
        lifespan=app_lifespan,
    )
    
    # ============================================================
    # CORS configuration
    # ============================================================
    
    allowed_origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    
    # fromenvironment variableaddextraallowsource
    extra_origins = os.environ.get("CORS_ORIGINS", "")
    if extra_origins:
        allowed_origins.extend([o.strip() for o in extra_origins.split(",") if o.strip()])
    
    # allowallsource（development/demouse）
    allow_all_origins = os.environ.get("CORS_ALLOW_ALL", "").lower() == "true"
    allow_credentials = not allow_all_origins
    if allow_all_origins:
        allowed_origins = ["*"]
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    add_auth_middleware(app)
    
    # ============================================================
    # registerroute
    # ============================================================
    
    app.include_router(api_v1_router)
    add_error_handlers(app)
    
    # ============================================================
    # rootrouteandhealthcheck
    # ============================================================
    
    has_frontend = static_dir.exists() and (static_dir / "index.html").exists()
    
    if has_frontend:
        @app.get("/", include_in_schema=False)
        async def root():
            """rootroute - returnfrontendpage"""
            return FileResponse(static_dir / "index.html")
    else:
        _FRONTEND_NOT_BUILT_HTML = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DSA - Frontend Not Built</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{min-height:100vh;display:flex;align-items:center;justify-content:center;
       background:#0a0e17;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,monospace}
  .card{max-width:580px;padding:2.5rem;border:1px solid #1e293b;border-radius:12px;background:#111827}
  h1{font-size:1.25rem;color:#38bdf8;margin-bottom:.75rem}
  p{font-size:.9rem;line-height:1.7;color:#94a3b8;margin-bottom:.5rem}
  code{background:#1e293b;padding:2px 8px;border-radius:4px;font-size:.85rem;color:#67e8f9}
  .hint{margin-top:1.25rem;padding:.75rem 1rem;border-left:3px solid #f59e0b;background:#1c1917;border-radius:0 6px 6px 0}
  .hint p{color:#fbbf24;margin:0}
  a{color:#38bdf8;text-decoration:none}
  a:hover{text-decoration:underline}
  .status{margin-top:1rem;font-size:.8rem;color:#475569}
</style></head><body><div class="card">
<h1>&#9888;&#65039; Frontend Not Built</h1>
<p>API is running, but the Web UI has not been built yet.</p>
<p>Build the frontend first:</p>
<p><code>cd apps/dsa-web &amp;&amp; npm install &amp;&amp; npm run build</code></p>
<p>Or start with auto-build:</p>
<p><code>python main.py --serve-only</code></p>
<div class="hint"><p>If you only need the API, visit <a href="/docs">/docs</a> for the interactive API documentation.</p></div>
<p class="status">API Version 1.0.0 &bull; <a href="/api/health">/api/health</a></p>
</div></body></html>"""

        @app.get("/", include_in_schema=False)
        async def root():
            """rootroute - frontendnotbuildreturn whenguidepage"""
            return HTMLResponse(content=_FRONTEND_NOT_BUILT_HTML)
    
    @app.get(
        "/api/health",
        response_model=HealthResponse,
        tags=["Health"],
        summary="healthcheck",
        description="forload balancinghandlerormonitorsystemcheckservicestatus"
    )
    async def health_check() -> HealthResponse:
        """healthcheckAPI/interface"""
        return HealthResponse(
            status="ok",
            timestamp=datetime.now().isoformat()
        )
    
    # ============================================================
    # staticfilehosting（frontend SPA）
    # ============================================================
    
    if has_frontend:
        # mountstaticcapitalsourcedirectory
        assets_dir = static_dir / "assets"
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
        
        # SPA routerollback
        @app.get("/{full_path:path}", include_in_schema=False)
        async def serve_spa(request: Request, full_path: str):
            """SPA routerollback - non- API routereturn index.html"""
            if full_path == "api" or full_path.startswith("api/"):
                return JSONResponse(
                    status_code=404,
                    content={"error": "not_found", "message": f"API endpoint /{full_path} not found"}
                )
            
            file_path = static_dir / full_path
            if file_path.exists() and file_path.is_file():
                # Issue #520: Explicitly resolve MIME type to avoid
                # browsers rejecting JS modules served as text/plain.
                content_type, _ = mimetypes.guess_type(str(file_path))
                return FileResponse(file_path, media_type=content_type)
            
            return FileResponse(static_dir / "index.html")
    
    return app


# defaultapplyinstance（provide uvicorn directlyuse）
app = create_app()
