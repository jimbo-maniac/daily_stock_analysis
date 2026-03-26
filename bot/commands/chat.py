# -*- coding: utf-8 -*-
"""
Chat command for free-form conversation with the Agent.
"""

import logging

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from src.config import get_config

logger = logging.getLogger(__name__)

class ChatCommand(BotCommand):
    """
    Chat command handler.
    
    Usage: /chat <message>
    Example: /chat help meanalyzingonebelowMaotairecenttrend
    """
    
    @property
    def name(self) -> str:
        return "chat"
        
    @property
    def description(self) -> str:
        return "with AI assistantproceedselfbyconversation (needenable Agent mode)"
        
    @property
    def usage(self) -> str:
        return "/chat <issue>"
        
    @property
    def aliases(self) -> list[str]:
        return ["c", "question"]
        
    def execute(self, message: BotMessage, args: list[str]) -> BotResponse:
        """Execute the chat command."""
        config = get_config()
        
        if not config.agent_mode:
            return BotResponse.text_response(
                "⚠️ Agent modenotenable，unable touseconversationfeature。\nplease inconfigurationinsettings `AGENT_MODE=true`。"
            )
            
        if not args:
            return BotResponse.text_response(
                "⚠️ pleaseprovideneedqueryquestionissue。\nusage: `/chat <issue>`\nExample: `/chat help meanalyzingonebelowMaotairecenttrend`"
            )
            
        user_message = " ".join(args)
        session_id = f"{message.platform}_{message.user_id}"
        
        try:
            from src.agent.factory import build_agent_executor
            executor = build_agent_executor(config)
            result = executor.chat(message=user_message, session_id=session_id)
            
            if result.success:
                return BotResponse.text_response(result.content)
            else:
                return BotResponse.text_response(f"⚠️ conversationfailed: {result.error}")
                
        except Exception as e:
            logger.error(f"Chat command failed: {e}")
            logger.exception("Chat error details:")
            return BotResponse.text_response(f"⚠️ conversationexecuteerror: {str(e)}")
