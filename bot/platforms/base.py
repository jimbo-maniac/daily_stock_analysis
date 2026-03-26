# -*- coding: utf-8 -*-
"""
===================================
platformadapterbaseclass
===================================

defineplatformadapterabstractionbaseclass，eachplatformmustinheritancethisclass。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple

from bot.models import BotMessage, BotResponse, WebhookResponse


class BotPlatform(ABC):
    """
    platformadapterabstractionbaseclass
    
    responsible for：
    1. verification Webhook requestsignature
    2. parsingplatformmessageto unifyformat
    3. willresponseconvertingasplatformformat
    
    useExample：
        class MyPlatform(BotPlatform):
            @property
            def platform_name(self) -> str:
                return "myplatform"
            
            def verify_request(self, headers, body) -> bool:
                # verificationsignaturelogic
                return True
            
            def parse_message(self, data) -> Optional[BotMessage]:
                # parsingmessagelogic
                return BotMessage(...)
            
            def format_response(self, response, message) -> WebhookResponse:
                # formattingresponselogic
                return WebhookResponse.success({"text": response.text})
    """
    
    @property
    @abstractmethod
    def platform_name(self) -> str:
        """
        platform identifiername
        
        forroutematchandlogidentifier，e.g. "feishu", "dingtalk"
        """
        pass
    
    @abstractmethod
    def verify_request(self, headers: Dict[str, str], body: bytes) -> bool:
        """
        verificationrequestsignature
        
        eachplatformhasnotsamesignatureverificationmechanism，needseparateimplement。
        
        Args:
            headers: HTTP request headers
            body: request bodyraw bytes
            
        Returns:
            signaturewhethervalid
        """
        pass
    
    @abstractmethod
    def parse_message(self, data: Dict[str, Any]) -> Optional[BotMessage]:
        """
        parsingplatformmessageto unifyformat
        
        willplatformspecificmessageformatconvertingas BotMessage。
        ifis notneedprocessingmessagetype（e.g.eventpullback），return None。
        
        Args:
            data: parsingafter JSON data
            
        Returns:
            BotMessage object，or None（no needprocessing）
        """
        pass
    
    @abstractmethod
    def format_response(
        self, 
        response: BotResponse, 
        message: BotMessage
    ) -> WebhookResponse:
        """
        willunifiedresponseconvertingasplatformformat
        
        Args:
            response: unifiedresponseobject
            message: rawmessageobject（forgetreplytargetetcinfo）
            
        Returns:
            WebhookResponse object
        """
        pass
    
    def handle_challenge(self, data: Dict[str, Any]) -> Optional[WebhookResponse]:
        """
        processingplatformverificationrequest
        
        partialplatforminconfiguration Webhook whenwillsendingverificationrequest，needreturnspecificresponse。
        sub-classcanoverridethismethod。
        
        Args:
            data: request countdata
            
        Returns:
            verificationresponse，or None（is notverificationrequest）
        """
        return None
    
    def handle_webhook(
        self, 
        headers: Dict[str, str], 
        body: bytes,
        data: Dict[str, Any]
    ) -> Tuple[Optional[BotMessage], Optional[WebhookResponse]]:
        """
        processing Webhook request
        
        thisismainentrymethod，coordinateverification、parsingetcprocess。
        
        Args:
            headers: HTTP request headers
            body: request bodyraw bytes
            data: parsingafter JSON data
            
        Returns:
            (BotMessage, WebhookResponse) tuple
            - if it isverificationrequest：(None, challenge_response)
            - if it isnormalmessage：(message, None) - responsewillincommandprocessingaftergenerating
            - ifverificationfailedorno need forprocessing：(None, error_response or None)
        """
        # 1. checkwhether isverificationrequest
        challenge_response = self.handle_challenge(data)
        if challenge_response:
            return None, challenge_response
        
        # 2. verificationrequestsignature
        if not self.verify_request(headers, body):
            return None, WebhookResponse.error("Invalid signature", 403)
        
        # 3. parsingmessage
        message = self.parse_message(data)
        
        return message, None
