# -*- coding: utf-8 -*-
"""
===================================
botmessagemodel
===================================

defineunifiedmessageandresponsemodel，blockeachplatformdifference。
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, List


class ChatType(str, Enum):
    """sessiontype"""
    GROUP = "group"      # group chat
    PRIVATE = "private"  # private message
    UNKNOWN = "unknown"  # unknown


class Platform(str, Enum):
    """platformtype"""
    FEISHU = "feishu"        # Feishu
    DINGTALK = "dingtalk"    # DingTalk
    WECOM = "wecom"          # Enterprise WeChat
    TELEGRAM = "telegram"    # Telegram
    UNKNOWN = "unknown"      # unknown


@dataclass
class BotMessage:
    """
    unifiedbotmessagemodel
    
    willeachplatformmessageformatunifiedasthismodel，for conveniencecommandhandlerprocessing。
    
    Attributes:
        platform: platform identifier
        message_id: message ID（platformraw ID）
        user_id: sendinger ID
        user_name: sendingername
        chat_id: session ID（group chat ID orprivate message ID）
        chat_type: sessiontype
        content: messagetextcontent（alreadyremove @bot partial）
        raw_content: rawmessagecontent
        mentioned: whether @bot
        mentions: @userlist
        timestamp: messagetimestamp
        raw_data: rawrequest countdata（platformspecific，fordebug）
    """
    platform: str
    message_id: str
    user_id: str
    user_name: str
    chat_id: str
    chat_type: ChatType
    content: str
    raw_content: str = ""
    mentioned: bool = False
    mentions: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    def get_command_and_args(self, prefix: str = "/") -> tuple:
        """
        parsingcommandandparameter
        
        Args:
            prefix: commandprefix，default "/"
            
        Returns:
            (command, args) tuple，e.g. ("analyze", ["600519"])
            ifis notcommand，return (None, [])
        """
        text = self.content.strip()
        
        # checkwhetherwithcommandprefixstart
        if not text.startswith(prefix):
            # trymatchChinesecommand（noprefix）
            chinese_commands = {
                'analyzing': 'analyze',
                'market index': 'market',
                'batch': 'batch',
                'help': 'help',
                'status': 'status',
            }
            for cn_cmd, en_cmd in chinese_commands.items():
                if text.startswith(cn_cmd):
                    args = text[len(cn_cmd):].strip().split()
                    return en_cmd, args
            return None, []
        
        # removeprefix
        text = text[len(prefix):]
        
        # splittingcommandandparameter
        parts = text.split()
        if not parts:
            return None, []
        
        command = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []
        
        return command, args
    
    def is_command(self, prefix: str = "/") -> bool:
        """checkmessagewhether iscommand"""
        cmd, _ = self.get_command_and_args(prefix)
        return cmd is not None


@dataclass
class BotResponse:
    """
    unifiedbotresponsemodel
    
    commandhandlerreturnthismodel，byplatformadapterconvertingasplatformspecificformat。
    
    Attributes:
        text: reply text
        markdown: whether is Markdown format
        at_user: whether @sendinger
        reply_to_message: whetherreplyoriginalmessage
        extra: extradata（platformspecific）
    """
    text: str
    markdown: bool = False
    at_user: bool = True
    reply_to_message: bool = True
    extra: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def text_response(cls, text: str, at_user: bool = True) -> 'BotResponse':
        """creatingplain textresponse"""
        return cls(text=text, markdown=False, at_user=at_user)
    
    @classmethod
    def markdown_response(cls, text: str, at_user: bool = True) -> 'BotResponse':
        """creating Markdown response"""
        return cls(text=text, markdown=True, at_user=at_user)
    
    @classmethod
    def error_response(cls, message: str) -> 'BotResponse':
        """creatingerrorresponse"""
        return cls(text=f"❌ error：{message}", markdown=False, at_user=True)


@dataclass
class WebhookResponse:
    """
    Webhook responsemodel
    
    platformadapterreturnthismodel，packageinclude HTTP responsecontent。
    
    Attributes:
        status_code: HTTP status code
        body: response body（dictionary，willby JSON sequencecolumn-ize）
        headers: extraresponse headers
    """
    status_code: int = 200
    body: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    
    @classmethod
    def success(cls, body: Optional[Dict] = None) -> 'WebhookResponse':
        """creatingsuccessfulresponse"""
        return cls(status_code=200, body=body or {})
    
    @classmethod
    def challenge(cls, challenge: str) -> 'WebhookResponse':
        """creatingverificationresponse（forplatform URL verification）"""
        return cls(status_code=200, body={"challenge": challenge})
    
    @classmethod
    def error(cls, message: str, status_code: int = 400) -> 'WebhookResponse':
        """creatingerrorresponse"""
        return cls(status_code=status_code, body={"error": message})
