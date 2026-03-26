# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - asynchronoustaskqueue
===================================

Responsibilities:
1. manageasynchronousanalyzingtasklifecycleperiod
2. preventsamestock codeduplicatesubmit
3. provide SSE eventbroadcastmechanism
4. taskcompletedafterpersisttodatabase
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List, Any, TYPE_CHECKING, Tuple, Literal

if TYPE_CHECKING:
    from asyncio import Queue as AsyncQueue

from data_provider.base import canonical_stock_code, normalize_stock_code
from src.utils.analysis_metadata import SELECTION_SOURCES

logger = logging.getLogger(__name__)


def _dedupe_stock_code_key(stock_code: str) -> str:
    """
    Build the internal duplicate-detection key for a stock code.

    The task queue should treat equivalent market code shapes as the same
    underlying stock, e.g. ``600519`` and ``600519.SH``.
    """
    return canonical_stock_code(normalize_stock_code(stock_code))


class TaskStatus(str, Enum):
    """Task status enumeration"""
    PENDING = "pending"        # Waiting for execution
    PROCESSING = "processing"  # In progress
    COMPLETED = "completed"    # Completed
    FAILED = "failed"          # Failed


@dataclass
class TaskInfo:
    """
    Task information dataclass.

    Used for API responses and internal task management.
    """
    task_id: str
    stock_code: str
    stock_name: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    report_type: str = "detailed"
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    original_query: Optional[str] = None
    selection_source: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert task info into an API-friendly dictionary."""
        return {
            "task_id": self.task_id,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "report_type": self.report_type,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "original_query": self.original_query,
            "selection_source": self.selection_source,
        }
    
    def copy(self) -> 'TaskInfo':
        """Create a shallow copy of the task information."""
        return TaskInfo(
            task_id=self.task_id,
            stock_code=self.stock_code,
            stock_name=self.stock_name,
            status=self.status,
            progress=self.progress,
            message=self.message,
            result=self.result,
            error=self.error,
            report_type=self.report_type,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            original_query=self.original_query,
            selection_source=self.selection_source,
        )


class DuplicateTaskError(Exception):
    """
    duplicatesubmitabnormal
    
    whenstockalreadyinanalyzinginraise whenthisabnormal
    """
    def __init__(self, stock_code: str, existing_task_id: str):
        self.stock_code = stock_code
        self.existing_task_id = existing_task_id
        super().__init__(f"stock {stock_code} currentlyanalyzingin (task_id: {existing_task_id})")


class AnalysisTaskQueue:
    """
    asynchronousanalyzingtaskqueue
    
    singleton pattern，globaluniqueinstance
    
    feature：
    1. preventsamestock codeduplicatesubmit
    2. threadpoolexecuteanalyzingtask
    3. SSE eventbroadcastmechanism
    4. taskcompletedafterautomaticpersist
    """
    
    _instance: Optional['AnalysisTaskQueue'] = None
    _instance_lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, max_workers: int = 3):
        # preventduplicateinitializing
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._max_workers = max_workers
        self._executor: Optional[ThreadPoolExecutor] = None
        
        # coredatastructure
        self._tasks: Dict[str, TaskInfo] = {}           # task_id -> TaskInfo
        self._analyzing_stocks: Dict[str, str] = {}     # dedupe_key -> task_id
        self._futures: Dict[str, Future] = {}           # task_id -> Future
        
        # SSE subscribeerlist（asyncio.Queue instance）
        self._subscribers: List['AsyncQueue'] = []
        self._subscribers_lock = threading.Lock()
        
        # maineventloopreference（forcrossthreadbroadcast）
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        
        # threadsafelock
        self._data_lock = threading.RLock()
        
        # taskhistoricalkeepquantity（instorein）
        self._max_history = 100
        
        self._initialized = True
        logger.info(f"[TaskQueue] initializingcompleted，maxconcurrency: {max_workers}")
    
    @property
    def executor(self) -> ThreadPoolExecutor:
        """lazyloadingthreadpool"""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="analysis_task_"
            )
        return self._executor

    @property
    def max_workers(self) -> int:
        """Return current executor max worker setting."""
        return self._max_workers

    def _has_inflight_tasks_locked(self) -> bool:
        """Check whether queue has any pending/processing tasks."""
        if self._analyzing_stocks:
            return True
        return any(
            task.status in (TaskStatus.PENDING, TaskStatus.PROCESSING)
            for task in self._tasks.values()
        )

    def sync_max_workers(
        self,
        max_workers: int,
        *,
        log: bool = True,
    ) -> Literal["applied", "unchanged", "deferred_busy"]:
        """
        Try to sync queue concurrency without replacing singleton instance.

        Returns:
            - "applied": new value applied immediately (idle queue only)
            - "unchanged": target equals current value or invalid target
            - "deferred_busy": queue is busy, apply is deferred
        """
        try:
            target = max(1, int(max_workers))
        except (TypeError, ValueError):
            if log:
                logger.warning("[TaskQueue] ignore non-method MAX_WORKERS value: %r", max_workers)
            return "unchanged"

        executor_to_shutdown: Optional[ThreadPoolExecutor] = None
        previous: int
        with self._data_lock:
            previous = self._max_workers
            if target == previous:
                return "unchanged"

            if self._has_inflight_tasks_locked():
                if log:
                    logger.info(
                        "[TaskQueue] maxconcurrencyadjustextendafter: currentbusy (%s -> %s)",
                        previous,
                        target,
                    )
                return "deferred_busy"

            self._max_workers = target
            executor_to_shutdown = self._executor
            self._executor = None

        if executor_to_shutdown is not None:
            executor_to_shutdown.shutdown(wait=False)

        if log:
            logger.info("[TaskQueue] maxconcurrencyalreadyupdating: %s -> %s", previous, target)
        return "applied"
    
    # ========== tasksubmitwithquerying ==========
    
    def is_analyzing(self, stock_code: str) -> bool:
        """
        checkstockwhether currentlyanalyzingin
        
        Args:
            stock_code: stock code
            
        Returns:
            True indicatescurrentlyanalyzingin
        """
        dedupe_key = _dedupe_stock_code_key(stock_code)
        with self._data_lock:
            return dedupe_key in self._analyzing_stocks
    
    def get_analyzing_task_id(self, stock_code: str) -> Optional[str]:
        """
        getcurrentlyanalyzingthisstocktask ID
        
        Args:
            stock_code: stock code
            
        Returns:
            task ID，if nothen return None
        """
        dedupe_key = _dedupe_stock_code_key(stock_code)
        with self._data_lock:
            return self._analyzing_stocks.get(dedupe_key)

    def validate_selection_source(self, selection_source: Optional[str]) -> None:
        """
        Validate the selection source parameter.

        Args:
            selection_source: Selection source label.

        Raises:
            ValueError: Raised when the selection source is invalid.
        """
        if selection_source is not None and selection_source not in SELECTION_SOURCES:
            raise ValueError(
                f"Invalid selection_source: {selection_source}. "
                f"Must be one of {SELECTION_SOURCES}"
            )
    
    def submit_task(
        self,
        stock_code: str,
        stock_name: Optional[str] = None,
        original_query: Optional[str] = None,
        selection_source: Optional[str] = None,
        report_type: str = "detailed",
        force_refresh: bool = False,
    ) -> TaskInfo:
        """
        Submit a single analysis task.

        Args:
            stock_code: Stock code
            stock_name: Optional stock name
            original_query: Optional raw user input
            selection_source: Optional source label
            report_type: Report type
            force_refresh: Whether to bypass cache

        Returns:
            TaskInfo: Accepted task information

        Raises:
            DuplicateTaskError: Raised when the stock is already being analyzed
        """
        stock_code = canonical_stock_code(stock_code)
        if not stock_code:
            raise ValueError("stock codecannotis emptyor onlypackageincluding whitespace")

        accepted, duplicates = self.submit_tasks_batch(
            [stock_code],
            stock_name=stock_name,
            original_query=original_query,
            selection_source=selection_source,
            report_type=report_type,
            force_refresh=force_refresh,
        )
        if duplicates:
            raise duplicates[0]
        return accepted[0]

    def submit_tasks_batch(
        self,
        stock_codes: List[str],
        stock_name: Optional[str] = None,
        original_query: Optional[str] = None,
        selection_source: Optional[str] = None,
        report_type: str = "detailed",
        force_refresh: bool = False,
    ) -> Tuple[List[TaskInfo], List[DuplicateTaskError]]:
        """
        Submit analysis tasks in batch.

        - Duplicate stocks are skipped and recorded in duplicates.
        - If executor submission fails, the current batch is rolled back.
        """
        self.validate_selection_source(selection_source)

        accepted: List[TaskInfo] = []
        duplicates: List[DuplicateTaskError] = []
        created_task_ids: List[str] = []

        canonical_codes = [
            normalized for normalized in (canonical_stock_code(code) for code in stock_codes)
            if normalized
        ]

        with self._data_lock:
            for stock_code in canonical_codes:
                dedupe_key = _dedupe_stock_code_key(stock_code)
                if dedupe_key in self._analyzing_stocks:
                    existing_task_id = self._analyzing_stocks[dedupe_key]
                    duplicates.append(DuplicateTaskError(stock_code, existing_task_id))
                    continue

                task_id = uuid.uuid4().hex
                task_info = TaskInfo(
                    task_id=task_id,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    status=TaskStatus.PENDING,
                    message="taskaddedqueue",
                    report_type=report_type,
                    original_query=original_query,
                    selection_source=selection_source,
                )
                self._tasks[task_id] = task_info
                self._analyzing_stocks[dedupe_key] = task_id

                try:
                    future = self.executor.submit(
                        self._execute_task,
                        task_id,
                        stock_code,
                        report_type,
                        force_refresh,
                    )
                except Exception:
                    # Roll back the current batch to avoid partial submission.
                    self._rollback_submitted_tasks_locked(created_task_ids + [task_id])
                    raise

                self._futures[task_id] = future
                accepted.append(task_info)
                created_task_ids.append(task_id)
                logger.info(f"[TaskQueue] taskalreadysubmit: {stock_code} -> {task_id}")

            # Keep task_created ordered before worker-emitted task_started/task_completed.
            # Broadcasting here also preserves batch rollback semantics because we only
            # reach this point after every submit in the batch has succeeded.
            for task_info in accepted:
                self._broadcast_event("task_created", task_info.to_dict())

        return accepted, duplicates

    def _rollback_submitted_tasks_locked(self, task_ids: List[str]) -> None:
        """rollbackcurrentbatchalreadycreatingbutstillnotstablereturngivecallmethodtask。"""
        for task_id in task_ids:
            future = self._futures.pop(task_id, None)
            if future is not None:
                future.cancel()

            task = self._tasks.pop(task_id, None)
            if task:
                dedupe_key = _dedupe_stock_code_key(task.stock_code)
                if self._analyzing_stocks.get(dedupe_key) == task_id:
                    del self._analyzing_stocks[dedupe_key]
    
    def get_task(self, task_id: str) -> Optional[TaskInfo]:
        """
        gettaskinfo
        
        Args:
            task_id: task ID
            
        Returns:
            TaskInfo or None
        """
        with self._data_lock:
            task = self._tasks.get(task_id)
            return task.copy() if task else None
    
    def list_pending_tasks(self) -> List[TaskInfo]:
        """
        get allin progresstask（pending + processing）
        
        Returns:
            tasklist（replica）
        """
        with self._data_lock:
            return [
                task.copy() for task in self._tasks.values()
                if task.status in (TaskStatus.PENDING, TaskStatus.PROCESSING)
            ]
    
    def list_all_tasks(self, limit: int = 50) -> List[TaskInfo]:
        """
        get alltask（bycreation timereverse order）
        
        Args:
            limit: return countconstraint
            
        Returns:
            tasklist（replica）
        """
        with self._data_lock:
            tasks = sorted(
                self._tasks.values(),
                key=lambda t: t.created_at,
                reverse=True
            )
            return [t.copy() for t in tasks[:limit]]
    
    def get_task_stats(self) -> Dict[str, int]:
        """
        gettaskstatistics
        
        Returns:
            statisticsdictionary
        """
        with self._data_lock:
            stats = {
                "total": len(self._tasks),
                "pending": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0,
            }
            for task in self._tasks.values():
                stats[task.status.value] = stats.get(task.status.value, 0) + 1
            return stats
    
    # ========== taskexecute ==========
    
    def _execute_task(
        self,
        task_id: str,
        stock_code: str,
        report_type: str,
        force_refresh: bool,
    ) -> Optional[Dict[str, Any]]:
        """
        executeanalyzingtask（inthreadpoolinrunning）
        
        Args:
            task_id: task ID
            stock_code: stock code
            report_type: report type
            force_refresh: whethermandatoryrefresh
            
        Returns:
            analysis resultdictionary
        """
        # updatingstatusasprocessingin
        with self._data_lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            task.status = TaskStatus.PROCESSING
            task.started_at = datetime.now()
            task.message = "currentlyanalyzingin..."
            task.progress = 10
        
        self._broadcast_event("task_started", task.to_dict())
        
        try:
            # importanalyzingservice（lazy import to avoid circulardependency）
            from src.services.analysis_service import AnalysisService
            
            # executeanalyzing
            service = AnalysisService()
            result = service.analyze_stock(
                stock_code=stock_code,
                report_type=report_type,
                force_refresh=force_refresh,
                query_id=task_id,
            )
            
            if result:
                # updatingtask statusascompleted
                with self._data_lock:
                    task = self._tasks.get(task_id)
                    if task:
                        task.status = TaskStatus.COMPLETED
                        task.progress = 100
                        task.completed_at = datetime.now()
                        task.result = result
                        task.message = "analyzingcompleted"
                        task.stock_name = result.get("stock_name", task.stock_name)
                        
                        # fromanalyzinginsetremove
                        dedupe_key = _dedupe_stock_code_key(task.stock_code)
                        if dedupe_key in self._analyzing_stocks:
                            del self._analyzing_stocks[dedupe_key]
                
                self._broadcast_event("task_completed", task.to_dict())
                logger.info(f"[TaskQueue] taskcompleted: {task_id} ({stock_code})")
                
                # clean expiredtask
                self._cleanup_old_tasks()
                
                return result
            else:
                # analyzingreturn empty result
                raise Exception("analyzingreturn empty result")
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"[TaskQueue] taskfailed: {task_id} ({stock_code}), error: {error_msg}")
            
            with self._data_lock:
                task = self._tasks.get(task_id)
                if task:
                    task.status = TaskStatus.FAILED
                    task.completed_at = datetime.now()
                    task.error = error_msg[:200]  # constrainterror messagelength
                    task.message = f"analyzingfailed: {error_msg[:50]}"
                    
                    # fromanalyzinginsetremove
                    dedupe_key = _dedupe_stock_code_key(task.stock_code)
                    if dedupe_key in self._analyzing_stocks:
                        del self._analyzing_stocks[dedupe_key]
            
            self._broadcast_event("task_failed", task.to_dict())
            
            # clean expiredtask
            self._cleanup_old_tasks()
            
            return None
    
    def _cleanup_old_tasks(self) -> int:
        """
        clean expiredcompletedtask
        
        keeprecent _max_history counttask
        
        Returns:
            cleantaskquantity
        """
        with self._data_lock:
            if len(self._tasks) <= self._max_history:
                return 0
            
            # bytimesorting，deletingoldcompletedtask
            completed_tasks = sorted(
                [t for t in self._tasks.values()
                 if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)],
                key=lambda t: t.created_at
            )
            
            to_remove = len(self._tasks) - self._max_history
            removed = 0
            
            for task in completed_tasks[:to_remove]:
                del self._tasks[task.task_id]
                if task.task_id in self._futures:
                    del self._futures[task.task_id]
                removed += 1
            
            if removed > 0:
                logger.debug(f"[TaskQueue] clean {removed} countperiodtask")
            
            return removed
    
    # ========== SSE eventbroadcast ==========
    
    def subscribe(self, queue: 'AsyncQueue') -> None:
        """
        subscribetaskevent
        
        Args:
            queue: asyncio.Queue instance，forreceivingevent
        """
        with self._subscribers_lock:
            self._subscribers.append(queue)
            # capturecurrenteventloop（shouldinmainthread async contextincall）
            try:
                self._main_loop = asyncio.get_running_loop()
            except RuntimeError:
                # ifnotin async contextin，trygeteventloop
                try:
                    self._main_loop = asyncio.get_event_loop()
                except RuntimeError:
                    pass
            logger.debug(f"[TaskQueue] newsubscribeeradd，currentsubscribeercount: {len(self._subscribers)}")
    
    def unsubscribe(self, queue: 'AsyncQueue') -> None:
        """
        cancelsubscribetaskevent
        
        Args:
            queue: needcancelsubscribe asyncio.Queue instance
        """
        with self._subscribers_lock:
            if queue in self._subscribers:
                self._subscribers.remove(queue)
                logger.debug(f"[TaskQueue] subscribeerleave，currentsubscribeercount: {len(self._subscribers)}")
    
    def _broadcast_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        broadcasteventtoallsubscribeer
        
        use call_soon_threadsafe ensurecrossthreadsafe
        
        Args:
            event_type: eventtype
            data: eventdata
        """
        event = {"type": event_type, "data": data}
        
        with self._subscribers_lock:
            subscribers = self._subscribers.copy()
            loop = self._main_loop
        
        if not subscribers:
            return
        
        if loop is None:
            logger.warning("[TaskQueue] unable tobroadcastevent：maineventloopnotsettings")
            return
        
        for queue in subscribers:
            try:
                # use call_soon_threadsafe willeventput into asyncio queue
                # thisisfromworkthreadtomaineventloopsendingmessagesafemethod
                loop.call_soon_threadsafe(queue.put_nowait, event)
            except RuntimeError as e:
                # eventloopalreadyclose
                logger.debug(f"[TaskQueue] broadcasteventskip（loopalreadyclose）: {e}")
            except Exception as e:
                logger.warning(f"[TaskQueue] broadcasteventfailed: {e}")
    
    # ========== cleanmethod ==========
    
    def shutdown(self) -> None:
        """closetaskqueue"""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
            logger.info("[TaskQueue] threadpoolalreadyclose")


# ========== convenientfunction ==========

def get_task_queue() -> AnalysisTaskQueue:
    """
    gettaskqueuesingleton
    
    Returns:
        AnalysisTaskQueue instance
    """
    queue = AnalysisTaskQueue()
    try:
        from src.config import get_config

        config = get_config()
        target_workers = max(1, int(getattr(config, "max_workers", queue.max_workers)))
        queue.sync_max_workers(target_workers, log=False)
    except Exception as exc:
        logger.debug("[TaskQueue] reading MAX_WORKERS failed，usecurrentconcurrencysettings: %s", exc)

    return queue
