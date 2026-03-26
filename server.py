# -*- coding: utf-8 -*-
"""
===================================
Daily Stock Analysis - FastAPI backendserviceentry
===================================

Responsibilities:
1. provide RESTful API service
2. configuration CORS crossdomainsupport
3. healthcheckAPI/interface
4. hostingfrontendstaticfile（productionmode）

startmethod：
    uvicorn server:app --reload --host 0.0.0.0 --port 8000
    
    oruse main.py:
    python main.py --serve-only      # only start API service
    python main.py --serve           # API service + executeanalyzing
"""

import logging

from src.config import setup_env, get_config
from src.logging_config import setup_logging

# initializingenvironment variablewithlog
setup_env()

config = get_config()
level_name = (config.log_level or "INFO").upper()
level = getattr(logging, level_name, logging.INFO)

setup_logging(
    log_prefix="api_server",
    console_level=level,
    extra_quiet_loggers=['uvicorn', 'fastapi'],
)

# from api.app importapplyinstance
from api.app import app  # noqa: E402

# export app provide uvicorn use
__all__ = ['app']


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
