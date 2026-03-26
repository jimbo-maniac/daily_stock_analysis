# -*- coding: utf-8 -*-
"""
===================================
commanddispatcher
===================================

responsible forparsingcommand、matchhandler、dispatchexecute。
"""

import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Type, Callable

from bot.models import BotMessage, BotResponse
from bot.commands.base import BotCommand

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    simplefrequencyconstrainthandler
    
    based onsmoothdynamicwindowalgorithm，constrainteachuserrequestfrequency。
    """
    
    def __init__(self, max_requests: int = 10, window_seconds: int = 60):
        """
        Args:
            max_requests: max within windowrequest count
            window_seconds: window time（seconds）
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[float]] = defaultdict(list)
    
    def is_allowed(self, user_id: str) -> bool:
        """
        checkuserwhetherallowrequest
        
        Args:
            user_id: useridentifier
            
        Returns:
            whetherallow
        """
        now = time.time()
        window_start = now - self.window_seconds
        
        # clean expiredrecord
        self._requests[user_id] = [
            t for t in self._requests[user_id] 
            if t > window_start
        ]
        
        # checkwhetherover limit
        if len(self._requests[user_id]) >= self.max_requests:
            return False
        
        # recordthistimesrequest
        self._requests[user_id].append(now)
        return True
    
    def get_remaining(self, user_id: str) -> int:
        """getremainingavailablerequest count"""
        now = time.time()
        window_start = now - self.window_seconds
        
        # clean expiredrecord
        self._requests[user_id] = [
            t for t in self._requests[user_id] 
            if t > window_start
        ]
        
        return max(0, self.max_requests - len(self._requests[user_id]))


class CommandDispatcher:
    """
    commanddispatcher
    
    Responsibilities:
    1. registerandmanagecommandhandler
    2. parsingmessageincommandandparameter
    3. dispatchcommandtotoshouldhandler
    4. processingunknowncommandanderror
    
    useExample：
        dispatcher = CommandDispatcher()
        dispatcher.register(AnalyzeCommand())
        dispatcher.register(HelpCommand())
        
        response = dispatcher.dispatch(message)
    """
    
    def __init__(
        self, 
        command_prefix: str = "/",
        rate_limit_requests: int = 10,
        rate_limit_window: int = 60,
        admin_users: Optional[List[str]] = None
    ):
        """
        Args:
            command_prefix: commandprefix，default "/"
            rate_limit_requests: frequencyconstraint：max within windowrequest count
            rate_limit_window: frequencyconstraint：window time（seconds）
            admin_users: adminuser ID list
        """
        self.command_prefix = command_prefix
        self.admin_users = set(admin_users or [])
        
        self._commands: Dict[str, BotCommand] = {}
        self._aliases: Dict[str, str] = {}
        self._rate_limiter = RateLimiter(rate_limit_requests, rate_limit_window)
        
        # pullbackfunction：gethelpcommandcommandlist
        self._help_command_getter: Optional[Callable] = None
    
    def register(self, command: BotCommand) -> None:
        """
        registercommand
        
        Args:
            command: commandinstance
        """
        name = command.name.lower()
        
        if name in self._commands:
            logger.warning(f"[Dispatcher] command '{name}' already exists，willbyoverride")
        
        self._commands[name] = command
        logger.debug(f"[Dispatcher] registercommand: {name}")
        
        # registeralias
        for alias in command.aliases:
            alias_lower = alias.lower()
            if alias_lower in self._aliases:
                logger.warning(f"[Dispatcher] alias '{alias_lower}' already exists，willbyoverride")
            self._aliases[alias_lower] = name
            logger.debug(f"[Dispatcher] registeralias: {alias_lower} -> {name}")
    
    def register_class(self, command_class: Type[BotCommand]) -> None:
        """
        registercommandclass（automaticinstance-ize）
        
        Args:
            command_class: commandclass
        """
        self.register(command_class())
    
    def unregister(self, name: str) -> bool:
        """
        logoutcommand
        
        Args:
            name: commandname
            
        Returns:
            whethersuccessfullogout
        """
        name = name.lower()
        
        if name not in self._commands:
            return False
        
        command = self._commands.pop(name)
        
        # removealias
        for alias in command.aliases:
            self._aliases.pop(alias.lower(), None)
        
        logger.debug(f"[Dispatcher] logoutcommand: {name}")
        return True
    
    def get_command(self, name: str) -> Optional[BotCommand]:
        """
        getcommand
        
        supportcommandnameandaliasquerying。
        
        Args:
            name: commandnameoralias
            
        Returns:
            commandinstance，or None
        """
        name = name.lower()
        
        # firstquerycommandname
        if name in self._commands:
            return self._commands[name]
        
        # againqueryalias
        if name in self._aliases:
            return self._commands.get(self._aliases[name])
        
        return None
    
    def list_commands(self, include_hidden: bool = False) -> List[BotCommand]:
        """
        columnoutallcommand
        
        Args:
            include_hidden: whetherpackageincludehidecommand
            
        Returns:
            commandlist
        """
        commands = list(self._commands.values())
        
        if not include_hidden:
            commands = [c for c in commands if not c.hidden]
        
        return sorted(commands, key=lambda c: c.name)
    
    def is_admin(self, user_id: str) -> bool:
        """checkuserwhether isadmin"""
        return user_id in self.admin_users
    
    def add_admin(self, user_id: str) -> None:
        """addadmin"""
        self.admin_users.add(user_id)
    
    def remove_admin(self, user_id: str) -> None:
        """removeadmin"""
        self.admin_users.discard(user_id)
    
    def dispatch(self, message: BotMessage) -> BotResponse:
        """
        dispatchmessagetotoshouldcommand
        
        Args:
            message: messageobject
            
        Returns:
            responseobject
        """
        # 1. checkfrequencyconstraint
        if not self._rate_limiter.is_allowed(message.user_id):
            remaining_time = self._rate_limiter.window_seconds
            return BotResponse.error_response(
                f"requestatfrequent，please {remaining_time} secondsafteragaintest"
            )
        
        # 2. parsingcommandandparameter
        cmd_name, args = message.get_command_and_args(self.command_prefix)
        
        if cmd_name is None:
            # is notcommand，checkwhether @bot
            if message.mentioned:
                return BotResponse.text_response(
                    "yougood！Iisstockanalyzingassistant。\n"
                    f"sending `{self.command_prefix}help` viewavailablecommand。"
                )
            # non-commandmessage，notprocessing
            return BotResponse.text_response("")
        
        logger.info(f"[Dispatcher] receivedcommand: {cmd_name}, parameter: {args}, user: {message.user_name}")
        
        # 3. findcommandhandler
        command = self.get_command(cmd_name)
        
        if command is None:
            return BotResponse.error_response(
                f"unknowncommand: {cmd_name}\n"
                f"sending `{self.command_prefix}help` viewavailablecommand。"
            )
        
        # 4. checkpermission
        if command.admin_only and not self.is_admin(message.user_id):
            return BotResponse.error_response("thiscommandrequires adminpermission")
        
        # 5. verificationparameter
        error_msg = command.validate_args(args)
        if error_msg:
            return BotResponse.error_response(
                f"{error_msg}\nusage: `{command.usage}`"
            )
        
        # 6. executecommand
        try:
            response = command.execute(message, args)
            logger.info(f"[Dispatcher] command {cmd_name} executesuccessful")
            return response
        except Exception as e:
            logger.error(f"[Dispatcher] command {cmd_name} executefailed: {e}")
            logger.exception(e)
            return BotResponse.error_response(f"commandexecutefailed: {str(e)[:100]}")
    
    def set_help_command_getter(self, getter: Callable) -> None:
        """
        settingshelpcommandcommandlistgethandler
        
        forlet HelpCommand getcommandlist。
        
        Args:
            getter: pullbackfunction，returncommandlist
        """
        self._help_command_getter = getter


# globaldispatcherinstance
_dispatcher: Optional[CommandDispatcher] = None


def get_dispatcher() -> CommandDispatcher:
    """
    get globaldispatcherinstance
    
    usesingleton pattern，firsttimescallautomatically wheninitializingandregisterallcommand。
    """
    global _dispatcher
    
    if _dispatcher is None:
        from src.config import get_config
        
        config = get_config()
        
        # creatingdispatcher
        _dispatcher = CommandDispatcher(
            command_prefix=getattr(config, 'bot_command_prefix', '/'),
            rate_limit_requests=getattr(config, 'bot_rate_limit_requests', 10),
            rate_limit_window=getattr(config, 'bot_rate_limit_window', 60),
            admin_users=getattr(config, 'bot_admin_users', []),
        )
        
        # automaticregisterallcommand
        from bot.commands import ALL_COMMANDS
        for command_class in ALL_COMMANDS:
            _dispatcher.register_class(command_class)
        
        logger.info(f"[Dispatcher] initializingcompleted，alreadyregister {len(_dispatcher._commands)} countcommand")
    
    return _dispatcher


def reset_dispatcher() -> None:
    """resetglobaldispatcher（mainly fortesting）"""
    global _dispatcher
    _dispatcher = None
