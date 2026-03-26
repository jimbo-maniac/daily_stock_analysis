# -*- coding: utf-8 -*-
"""
===================================
fixedwhenschedulemodule
===================================

Responsibilities:
1. supportdailyfixedwhenexecutestockanalyzing
2. supportfixedwhenexecute market review
3. gracefulprocessingsignal，ensurecanrely onlogout

dependency：
- schedule: lightweightscheduled tasklibrary
"""

import logging
import signal
import sys
import time
import threading
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """
    gracefullogouthandler
    
    capture SIGTERM/SIGINT signal，ensuretaskcompletedafteragainlogout
    """
    
    def __init__(self):
        self.shutdown_requested = False
        self._lock = threading.Lock()
        
        # registersignalhandler
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """signalprocessingfunction"""
        with self._lock:
            if not self.shutdown_requested:
                logger.info(f"receivedlogoutsignal ({signum})，waitingcurrenttaskcompleted...")
                self.shutdown_requested = True
    
    @property
    def should_shutdown(self) -> bool:
        """checkwhethershouldthislogout"""
        with self._lock:
            return self.shutdown_requested


class Scheduler:
    """
    scheduled taskscheduler
    
    based on schedule libraryimplement，support：
    - dailyfixedwhenexecute
    - startwhenimmediatelyexecute
    - gracefullogout
    """
    
    def __init__(self, schedule_time: str = "18:00"):
        """
        initializingscheduler
        
        Args:
            schedule_time: dailyexecution time，format "HH:MM"
        """
        try:
            import schedule
            self.schedule = schedule
        except ImportError:
            logger.error("schedule librarynotsetup，pleaseexecute: pip install schedule")
            raise ImportError("pleasesetup schedule library: pip install schedule")
        
        self.schedule_time = schedule_time
        self.shutdown_handler = GracefulShutdown()
        self._task_callback: Optional[Callable] = None
        self._running = False
        
    def set_daily_task(self, task: Callable, run_immediately: bool = True):
        """
        settingsdailyscheduled task
        
        Args:
            task: needexecutetaskfunction（noparameter）
            run_immediately: whether insettingsafterimmediatelyexecuteonce
        """
        self._task_callback = task
        
        # settingsdailyscheduled task
        self.schedule.every().day.at(self.schedule_time).do(self._safe_run_task)
        logger.info(f"alreadysettingsdailyscheduled task，execution time: {self.schedule_time}")
        
        if run_immediately:
            logger.info("immediatelyexecuteoncetask...")
            self._safe_run_task()
    
    def _safe_run_task(self):
        """safeexecutetask（withabnormalcapture）"""
        if self._task_callback is None:
            return
        
        try:
            logger.info("=" * 50)
            logger.info(f"scheduled taskstartingexecute - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 50)
            
            self._task_callback()
            
            logger.info(f"scheduled taskexecutecompleted - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            logger.exception(f"scheduled taskexecutefailed: {e}")
    
    def run(self):
        """
        runningschedulermainloop
        
        blockingrunning，directtoreceivedlogoutsignal
        """
        self._running = True
        logger.info("schedulerstartingrunning...")
        logger.info(f"belowtimesexecution time: {self._get_next_run_time()}")
        
        while self._running and not self.shutdown_handler.should_shutdown:
            self.schedule.run_pending()
            time.sleep(30)  # each30secondscheckonce
            
            # eachhoursprintonceheartbeat
            if datetime.now().minute == 0 and datetime.now().second < 30:
                logger.info(f"schedulerrunning... next execution: {self._get_next_run_time()}")
        
        logger.info("schedulerstopped")
    
    def _get_next_run_time(self) -> str:
        """getbelowtimesexecution time"""
        jobs = self.schedule.get_jobs()
        if jobs:
            next_run = min(job.next_run for job in jobs)
            return next_run.strftime('%Y-%m-%d %H:%M:%S')
        return "notsettings"
    
    def stop(self):
        """stoppingscheduler"""
        self._running = False


def run_with_schedule(
    task: Callable,
    schedule_time: str = "18:00",
    run_immediately: bool = True
):
    """
    convenientfunction：usefixedwhenschedulerunningtask
    
    Args:
        task: needexecutetaskfunction
        schedule_time: dailyexecution time
        run_immediately: whetherimmediatelyexecuteonce
    """
    scheduler = Scheduler(schedule_time=schedule_time)
    scheduler.set_daily_task(task, run_immediately=run_immediately)
    scheduler.run()


if __name__ == "__main__":
    # testingfixedwhenschedule
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    )
    
    def test_task():
        print(f"taskexecutein... {datetime.now()}")
        time.sleep(2)
        print("taskcompleted!")
    
    print("starttestingscheduler（by Ctrl+C logout）")
    run_with_schedule(test_task, schedule_time="23:59", run_immediately=True)
