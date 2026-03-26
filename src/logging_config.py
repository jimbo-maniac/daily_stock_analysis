# -*- coding: utf-8 -*-
"""
===================================
logconfigurationmodule - unifiedlogsysteminitializing
===================================

Responsibilities:
1. provide unifiedlogformatandconfigurationconstant
2. supportconsole + file（regular/debug）threelayerlogoutput
3. automaticdecreaselowthethird-partylibraryloglevel
"""

import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(pathname)s:%(lineno)d | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class RelativePathFormatter(logging.Formatter):
    """custom Formatter，outputmutualtopathinstead ofabsolutelytopath"""

    def __init__(self, fmt=None, datefmt=None, relative_to=None):
        super().__init__(fmt, datefmt)
        self.relative_to = Path(relative_to) if relative_to else Path.cwd()

    def format(self, record):
        # willabsolutelytopathconvertasmutualtopath
        try:
            record.pathname = str(Path(record.pathname).relative_to(self.relative_to))
        except ValueError:
            # ifunable toconvertingasmutualtopath，maintainoriginallike
            pass
        return super().format(record)



# defaultneeddecreaselowloglevelthethird-partylibrary
DEFAULT_QUIET_LOGGERS = [
    'urllib3',
    'sqlalchemy',
    'google',
    'httpx',
]


def setup_logging(
    log_prefix: str = "app",
    log_dir: str = "./logs",
    console_level: Optional[int] = None,
    debug: bool = False,
    extra_quiet_loggers: Optional[List[str]] = None,
) -> None:
    """
    unifiedlogsysteminitializing

    configurationthreelayerlogoutput：
    1. console：based on debug parameteror console_level settingslevel
    2. regularlog file：INFO level，10MB rotation，keep 5 countbackup
    3. debuglog file：DEBUG level，50MB rotation，keep 3 countbackup

    Args:
        log_prefix: log filenameprefix（e.g. "api_server" -> api_server_20240101.log）
        log_dir: log filedirectory，default ./logs
        console_level: consoleloglevel（optional，priorityat debug parameter）
        debug: whetherenableddebugmode（consoleoutput DEBUG level）
        extra_quiet_loggers: extraneeddecreaselowloglevelthethird-partylibrarylist
    """
    # determineconsoleloglevel
    if console_level is not None:
        level = console_level
    else:
        level = logging.DEBUG if debug else logging.INFO

    # creatinglogdirectory
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # log filepath（by dateminutefile）
    today_str = datetime.now().strftime('%Y%m%d')
    log_file = log_path / f"{log_prefix}_{today_str}.log"
    debug_log_file = log_path / f"{log_prefix}_debug_{today_str}.log"

    # configurationroot logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # root logger set to DEBUG，by handler controloutputlevel

    # clearexisting handler，avoid duplicateadd
    if root_logger.handlers:
        root_logger.handlers.clear()
    # creatingmutualtopath Formatter（mutualtoatitemitemrootdirectory）
    project_root = Path.cwd()
    rel_formatter = RelativePathFormatter(
        LOG_FORMAT, LOG_DATE_FORMAT, relative_to=project_root
    )
    # Handler 1: consoleoutput
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(rel_formatter)
    root_logger.addHandler(console_handler)

    # Handler 2: regularlog file（INFO level，10MB rotation）
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(rel_formatter)
    root_logger.addHandler(file_handler)

    # Handler 3: debuglog file（DEBUG level，packageincluding alldetailed information）
    debug_handler = RotatingFileHandler(
        debug_log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=3,
        encoding='utf-8'
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(rel_formatter)
    root_logger.addHandler(debug_handler)

    # decreaselowthethird-partylibraryloglevel
    quiet_loggers = DEFAULT_QUIET_LOGGERS.copy()
    if extra_quiet_loggers:
        quiet_loggers.extend(extra_quiet_loggers)

    for logger_name in quiet_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # outputinitializingcompletedinfo（usemutualtopath）
    try:
        rel_log_path = log_path.resolve().relative_to(project_root)
    except ValueError:
        rel_log_path = log_path

    try:
        rel_log_file = log_file.resolve().relative_to(project_root)
    except ValueError:
        rel_log_file = log_file

    try:
        rel_debug_log_file = debug_log_file.resolve().relative_to(project_root)
    except ValueError:
        rel_debug_log_file = debug_log_file

    logging.info(f"logsysteminitializingcompleted，logdirectory: {rel_log_path}")
    logging.info(f"regularlog: {rel_log_file}")
    logging.info(f"debuglog: {rel_debug_log_file}")
