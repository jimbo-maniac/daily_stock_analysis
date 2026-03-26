# -*- coding: utf-8 -*-
"""
===================================
API dependencyinjectmodule
===================================

Responsibilities:
1. providedatabase Session dependency
2. provideconfigurationdependency
3. provideservicelayerdependency
"""

from typing import Generator

from fastapi import Request
from sqlalchemy.orm import Session

from src.storage import DatabaseManager
from src.config import get_config, Config
from src.services.system_config_service import SystemConfigService


def get_db() -> Generator[Session, None, None]:
    """
    getdatabase Session dependency
    
    use FastAPI dependencyinjectmechanism，ensurerequestendafterautomaticclose Session
    
    Yields:
        Session: SQLAlchemy Session object
        
    Example:
        @router.get("/items")
        async def get_items(db: Session = Depends(get_db)):
            ...
    """
    db_manager = DatabaseManager.get_instance()
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()


def get_config_dep() -> Config:
    """
    getconfigurationdependency
    
    Returns:
        Config: configurationsingletonobject
    """
    return get_config()


def get_database_manager() -> DatabaseManager:
    """
    getdatabasemanagerdependency
    
    Returns:
        DatabaseManager: databasemanagersingletonobject
    """
    return DatabaseManager.get_instance()


def get_system_config_service(request: Request) -> SystemConfigService:
    """Get app-lifecycle shared SystemConfigService instance."""
    service = getattr(request.app.state, "system_config_service", None)
    if service is None:
        service = SystemConfigService()
        request.app.state.system_config_service = service
    return service
