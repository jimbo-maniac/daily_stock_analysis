# -*- coding: utf-8 -*-
"""
===================================
Discord platformadapter
===================================

responsible for：
1. verification Discord Webhook request
2. parsing Discord messageto unifyformat
3. willresponseconvertingas Discord format
"""

import logging
from typing import Dict, Any, Optional

from bot.platforms.base import BotPlatform
from bot.models import BotMessage, WebhookResponse


logger = logging.getLogger(__name__)


class DiscordPlatform(BotPlatform):
    """Discord platformadapter"""
    
    @property
    def platform_name(self) -> str:
        """platform identifiername"""
        return "discord"
    
    def verify_request(self, headers: Dict[str, str], body: bytes) -> bool:
        """verification Discord Webhook requestsignature
        
        Discord Webhook signatureverification：
        1. fromrequest headersget X-Signature-Ed25519 and X-Signature-Timestamp
        2. usepublic keyverificationsignature
        
        Args:
            headers: HTTP request headers
            body: request bodyraw bytes
            
        Returns:
            signaturewhethervalid
        """
        # TODO: implement Discord Webhook signatureverification
        # currenttemporarilyreturn when True，aftercontinueneedimprove
        return True
    
    def parse_message(self, data: Dict[str, Any]) -> Optional[BotMessage]:
        """parsing Discord messageto unifyformat
        
        Args:
            data: parsingafter JSON data
            
        Returns:
            BotMessage object，or None（no needprocessing）
        """
        # checkwhether ismessageevent
        if data.get("type") != 1 and data.get("type") != 2:
            return None
        
        # mentioncancelinterestcontent
        content = data.get("content", "").strip()
        if not content:
            return None
        
        # extractuserinfo
        author = data.get("author", {})
        user_id = author.get("id", "")
        user_name = author.get("username", "unknown")
        
        # extractchannelinfo
        channel_id = data.get("channel_id", "")
        guild_id = data.get("guild_id", "")
        
        # mentioncancelinterest ID
        message_id = data.get("id", "")
        
        # extractattachitemsinfo（if exists）
        attachments = data.get("attachments", [])
        attachment_urls = [att["url"] for att in attachments if "url" in att]
        
        # build BotMessage object
        message = BotMessage(
            platform="discord",
            message_id=message_id,
            user_id=user_id,
            user_name=user_name,
            content=content,
            attachment_urls=attachment_urls,
            channel_id=channel_id,
            group_id=guild_id,
            # from data extract fromotherrelated information
            timestamp=data.get("timestamp"),
            mention_everyone=data.get("mention_everyone", False),
            mentions=data.get("mentions", []),
            
            # add Discord specificrawdata
            raw_data={
                "message_id": message_id,
                "channel_id": channel_id,
                "guild_id": guild_id,
                "author": author,
                "content": content,
                "timestamp": data.get("timestamp"),
                "attachments": attachments,
                "mentions": data.get("mentions", []),
                "mention_roles": data.get("mention_roles", []),
                "mention_everyone": data.get("mention_everyone", False),
                "type": data.get("type"),
            }
        )
        
        return message
    
    def format_response(self, response: Any, message: BotMessage) -> WebhookResponse:
        """willunifiedresponseconvertingas Discord format
        
        Args:
            response: unifiedresponseobject
            message: rawmessageobject
            
        Returns:
            WebhookResponse object
        """
        # build Discord responseformat
        discord_response = {
            "content": response.text if hasattr(response, "text") else str(response),
            "tts": False,
            "embeds": [],
            "allowed_mentions": {
                "parse": ["users", "roles", "everyone"]
            }
        }
        
        return WebhookResponse.success(discord_response)
    
    def handle_challenge(self, data: Dict[str, Any]) -> Optional[WebhookResponse]:
        """processing Discord verificationrequest
        
        Discord inconfiguration Webhook whenwillsendingverificationrequest
        
        Args:
            data: request countdata
            
        Returns:
            verificationresponse，or None（is notverificationrequest）
        """
        # Discord Webhook verificationrequesttypeis 1
        if data.get("type") == 1:
            return WebhookResponse.success({
                "type": 1
            })
        
        # Discord commandexchangemutualverification
        if "challenge" in data:
            return WebhookResponse.success({
                "challenge": data["challenge"]
            })
        
        return None
