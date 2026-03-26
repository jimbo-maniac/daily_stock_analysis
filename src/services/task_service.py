# -*- coding: utf-8 -*-
"""
===================================
asynchronoustaskservicelayer
===================================

Responsibilities:
1. manageasynchronousanalyzingtask（threadpool）
2. executestockanalyzingandpushresult
3. queryingtask statusand historical

migratemoveself web/services.py  AnalysisService class
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Dict, Any, List, Union

from src.enums import ReportType
from src.storage import get_db
from bot.models import BotMessage

logger = logging.getLogger(__name__)


class TaskService:
    """
    asynchronoustaskservice

    responsible for：
    1. manageasynchronousanalyzingtask
    2. executestockanalyzing
    3. triggernotificationpush
    """

    _instance: Optional['TaskService'] = None
    _lock = threading.Lock()

    def __init__(self, max_workers: int = 3):
        self._executor: Optional[ThreadPoolExecutor] = None
        self._max_workers = max_workers
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._tasks_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> 'TaskService':
        """getsingletoninstance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @property
    def executor(self) -> ThreadPoolExecutor:
        """getorcreatingthreadpool"""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="analysis_"
            )
        return self._executor

    def submit_analysis(
        self,
        code: str,
        report_type: Union[ReportType, str] = ReportType.SIMPLE,
        source_message: Optional[BotMessage] = None,
        save_context_snapshot: Optional[bool] = None,
        query_source: str = "bot"
    ) -> Dict[str, Any]:
        """
        submitasynchronousanalyzingtask

        Args:
            code: stock code
            report_type: Report type enum
            source_message: sourcemessage（forreply）
            save_context_snapshot: whethersavingcontextsnapshot
            query_source: tasksourceidentifier（bot/api/cli/system）

        Returns:
            taskinfodictionary
        """
        # ensure report_type isenumtype
        if isinstance(report_type, str):
            report_type = ReportType.from_str(report_type)

        task_id = f"{code}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

        # submittothreadpool
        self.executor.submit(
            self._run_analysis,
            code,
            task_id,
            report_type,
            source_message,
            save_context_snapshot,
            query_source
        )

        logger.info(f"[TaskService] alreadysubmitstock {code} analyzingtask, task_id={task_id}, report_type={report_type.value}")

        return {
            "success": True,
            "message": "analyzingtaskalreadysubmit，willasynchronousexecuteandpushnotification",
            "code": code,
            "task_id": task_id,
            "report_type": report_type.value
        }

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """gettask status"""
        with self._tasks_lock:
            return self._tasks.get(task_id)

    def list_tasks(self, limit: int = 20) -> List[Dict[str, Any]]:
        """columnoutrecenttask"""
        with self._tasks_lock:
            tasks = list(self._tasks.values())
        # bystart timereverse order
        tasks.sort(key=lambda x: x.get('start_time', ''), reverse=True)
        return tasks[:limit]

    def get_analysis_history(
        self,
        code: Optional[str] = None,
        query_id: Optional[str] = None,
        days: int = 30,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """getanalyzinghistoricalrecord"""
        db = get_db()
        records = db.get_analysis_history(code=code, query_id=query_id, days=days, limit=limit)
        return [r.to_dict() for r in records]

    def _run_analysis(
        self,
        code: str,
        task_id: str,
        report_type: ReportType = ReportType.SIMPLE,
        source_message: Optional[BotMessage] = None,
        save_context_snapshot: Optional[bool] = None,
        query_source: str = "bot"
    ) -> Dict[str, Any]:
        """
        executesinglestockanalyzing

        internalmethod，inthreadpoolinrunning
        """
        # initializingtask status
        with self._tasks_lock:
            self._tasks[task_id] = {
                "task_id": task_id,
                "code": code,
                "status": "running",
                "start_time": datetime.now().isoformat(),
                "result": None,
                "error": None,
                "report_type": report_type.value
            }

        try:
            # lazy import to avoid circulardependency
            from src.config import get_config
            from main import StockAnalysisPipeline

            logger.info(f"[TaskService] startinganalyzingstock: {code}")

            # creatinganalyzingpipeline
            config = get_config()
            pipeline = StockAnalysisPipeline(
                config=config,
                max_workers=1,
                source_message=source_message,
                query_id=task_id,
                query_source=query_source,
                save_context_snapshot=save_context_snapshot
            )

            # executesinglestockanalyzing（enabledsingle stockpush）
            result = pipeline.process_single_stock(
                code=code,
                skip_analysis=False,
                single_stock_notify=True,
                report_type=report_type
            )

            if result:
                result_data = {
                    "code": result.code,
                    "name": result.name,
                    "sentiment_score": result.sentiment_score,
                    "operation_advice": result.operation_advice,
                    "trend_prediction": result.trend_prediction,
                    "analysis_summary": result.analysis_summary,
                }

                with self._tasks_lock:
                    self._tasks[task_id].update({
                        "status": "completed",
                        "end_time": datetime.now().isoformat(),
                        "result": result_data
                    })

                logger.info(f"[TaskService] stock {code} analyzingcompleted: {result.operation_advice}")
                return {"success": True, "task_id": task_id, "result": result_data}
            else:
                with self._tasks_lock:
                    self._tasks[task_id].update({
                        "status": "failed",
                        "end_time": datetime.now().isoformat(),
                        "error": "analyzingreturn empty result"
                    })

                logger.warning(f"[TaskService] stock {code} analyzingfailed: return empty result")
                return {"success": False, "task_id": task_id, "error": "analyzingreturn empty result"}

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[TaskService] stock {code} analyzingabnormal: {error_msg}")

            with self._tasks_lock:
                self._tasks[task_id].update({
                    "status": "failed",
                    "end_time": datetime.now().isoformat(),
                    "error": error_msg
                })

            return {"success": False, "task_id": task_id, "error": error_msg}


# ============================================================
# convenientfunction
# ============================================================

def get_task_service() -> TaskService:
    """gettaskservicesingleton"""
    return TaskService.get_instance()
