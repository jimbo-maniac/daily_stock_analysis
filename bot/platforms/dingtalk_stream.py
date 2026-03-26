# -*- coding: utf-8 -*-
"""
===================================
DingTalk Stream modeadapter
===================================

useDingTalkofficial Stream SDK connectbot，no public network needed IP and Webhook configuration。

advantage：
- no needpublic network IP ordomainname
- no needconfiguration Webhook URL
- via WebSocket longconnectingreceivingmessage
- moresimpleconnectmethod

dependency：
pip install dingtalk-stream

DingTalk Stream SDK：
https://github.com/open-dingtalk/dingtalk-stream-sdk-python
"""

import logging
import asyncio
import threading
from datetime import datetime
from typing import Optional, Callable, Any

logger = logging.getLogger(__name__)

# tryimportDingTalk Stream SDK
try:
    import dingtalk_stream
    from dingtalk_stream import AckMessage

    DINGTALK_STREAM_AVAILABLE = True
except ImportError:
    DINGTALK_STREAM_AVAILABLE = False
    logger.warning("[DingTalk Stream] dingtalk-stream SDK notsetup，Stream modeunavailable")
    logger.warning("[DingTalk Stream] pleaserunning: pip install dingtalk-stream")

from bot.models import BotMessage, BotResponse, ChatType


class DingtalkStreamHandler:
    """
    DingTalk Stream modemessagehandler
    
    will Stream SDK pullbackconvertingto unify BotMessage format，
    and callcommanddispatcherprocessing。
    """

    def __init__(self, on_message: Callable[[BotMessage], BotResponse]):
        """
        Args:
            on_message: messageprocessingpullbackfunction，receiving BotMessage return BotResponse
        """
        self._on_message = on_message
        self._logger = logger

    @staticmethod
    def _truncate_log_content(text: str, max_len: int = 200) -> str:
        cleaned = text.replace("\n", " ").strip()
        if len(cleaned) > max_len:
            return f"{cleaned[:max_len]}..."
        return cleaned

    def _log_incoming_message(self, message: BotMessage) -> None:
        content = message.raw_content or message.content or ""
        summary = self._truncate_log_content(content)
        self._logger.info(
            "[DingTalk Stream] Incoming message: msg_id=%s user_id=%s chat_id=%s chat_type=%s content=%s",
            message.message_id,
            message.user_id,
            message.chat_id,
            getattr(message.chat_type, "value", message.chat_type),
            summary,
        )

    if DINGTALK_STREAM_AVAILABLE:
        class _ChatbotHandler(dingtalk_stream.ChatbotHandler):
            """internalmessagehandler"""

            def __init__(self, parent: 'DingtalkStreamHandler'):
                super().__init__()
                self._parent = parent
                self.logger = logger

            async def process(self, callback: dingtalk_stream.CallbackMessage):
                """processingreceivedmessage"""
                try:
                    # parsingmessage
                    incoming = dingtalk_stream.ChatbotMessage.from_dict(callback.data)

                    # convertingto unifyformat
                    bot_message = self._parent._parse_stream_message(incoming, callback.data)

                    if bot_message:
                        self._parent._log_incoming_message(bot_message)
                        # callmessageprocessingpullback
                        response = self._parent._on_message(bot_message)

                        # sendingreply
                        if response and response.text:
                            # build @user prefix（group chatscenariobelowneedintextinpackageinclude @username）
                            if response.at_user and incoming.sender_nick:
                                if response.markdown:
                                    self.reply_markdown(
                                        title="stockanalyzingassistant",
                                        text=f"@{incoming.sender_nick} " + response.text,
                                        incoming_message=incoming
                                    )
                                else:
                                    self.reply_text(response.text, incoming)

                    return AckMessage.STATUS_OK, 'OK'

                except Exception as e:
                    self.logger.error(f"[DingTalk Stream] processingmessagefailed: {e}")
                    self.logger.exception(e)
                    return AckMessage.STATUS_SYSTEM_EXCEPTION, str(e)

        def create_handler(self) -> '_ChatbotHandler':
            """creating SDK needhandlerinstance"""
            return self._ChatbotHandler(self)

    def _parse_stream_message(self, incoming: Any, raw_data: dict) -> Optional[BotMessage]:
        """
        parsing Stream messageto unifyformat
        
        Args:
            incoming: ChatbotMessage object
            raw_data: rawpullbackdata
        """
        try:
            raw_data = dict(raw_data or {})

            # getcancelinterestcontent
            raw_content = incoming.text.content if incoming.text else ''

            # extractcommand（remove @bot）
            content = self._extract_command(raw_content)

            # sessiontype
            conversation_type = getattr(incoming, 'conversation_type', None)
            if conversation_type == '1':
                chat_type = ChatType.PRIVATE
            elif conversation_type == '2':
                chat_type = ChatType.GROUP
            else:
                chat_type = ChatType.UNKNOWN

            # whether @bot（Stream modebelowreceivedmessagegeneralallis @bot）
            mentioned = True

            # extract sessionWebhook，for convenienceasynchronouspush
            session_webhook = (
                    getattr(incoming, 'session_webhook', None)
                    or raw_data.get('sessionWebhook')
                    or raw_data.get('session_webhook')
            )
            if session_webhook:
                raw_data['_session_webhook'] = session_webhook

            return BotMessage(
                platform='dingtalk',
                message_id=getattr(incoming, 'msg_id', '') or '',
                user_id=getattr(incoming, 'sender_id', '') or '',
                user_name=getattr(incoming, 'sender_nick', '') or '',
                chat_id=getattr(incoming, 'conversation_id', '') or '',
                chat_type=chat_type,
                content=content,
                raw_content=raw_content,
                mentioned=mentioned,
                mentions=[],
                timestamp=datetime.now(),
                raw_data=raw_data,
            )

        except Exception as e:
            logger.error(f"[DingTalk Stream] parsingmessagefailed: {e}")
            return None

    def _extract_command(self, text: str) -> str:
        """extractcommandcontent（remove @bot）"""
        import re
        text = re.sub(r'^@[\S]+\s*', '', text.strip())
        return text.strip()


class DingtalkStreamClient:
    """
    DingTalk Stream modeclient
    
    encapsulation dingtalk-stream SDK，providesimplestartAPI/interface。
    
    usage：
        client = DingtalkStreamClient()
        client.start()  # blockingrunning
        
        # orerin backgroundrunning
        client.start_background()
    """

    def __init__(
            self,
            client_id: Optional[str] = None,
            client_secret: Optional[str] = None
    ):
        """
        Args:
            client_id: apply AppKey（if not provided, get fromconfigurationreading）
            client_secret: apply AppSecret（if not provided, get fromconfigurationreading）
        """
        if not DINGTALK_STREAM_AVAILABLE:
            raise ImportError(
                "dingtalk-stream SDK notsetup。\n"
                "pleaserunning: pip install dingtalk-stream"
            )

        from src.config import get_config
        config = get_config()

        self._client_id = client_id or getattr(config, 'dingtalk_app_key', None)
        self._client_secret = client_secret or getattr(config, 'dingtalk_app_secret', None)

        if not self._client_id or not self._client_secret:
            raise ValueError(
                "DingTalk Stream modeneedconfiguration DINGTALK_APP_KEY and DINGTALK_APP_SECRET"
            )

        self._client: Optional[dingtalk_stream.DingTalkStreamClient] = None
        self._background_thread: Optional[threading.Thread] = None
        self._running = False

    def _create_message_handler(self) -> Callable[[BotMessage], BotResponse]:
        """creatingmessageprocessingfunction"""

        def handle_message(message: BotMessage) -> BotResponse:
            from bot.dispatcher import get_dispatcher
            dispatcher = get_dispatcher()
            return dispatcher.dispatch(message)

        return handle_message

    def start(self) -> None:
        """
        start Stream client（blocking）
        
        thismethodwillblockingcurrentthread，directtoclientstopping。
        """
        logger.info("[DingTalk Stream] starting...")

        # creatingcredential
        credential = dingtalk_stream.Credential(
            self._client_id,
            self._client_secret
        )

        # creatingclient
        self._client = dingtalk_stream.DingTalkStreamClient(credential)

        # registermessagehandler
        handler = DingtalkStreamHandler(self._create_message_handler())
        self._client.register_callback_handler(
            dingtalk_stream.chatbot.ChatbotMessage.TOPIC,
            handler.create_handler()
        )

        self._running = True
        logger.info("[DingTalk Stream] clientstarted，waitingmessage...")

        # start（blocking）
        self._client.start_forever()

    def start_background(self) -> None:
        """
        in backgroundthreadstart Stream client（non-blocking）
        
        suitableforwithotherservice（e.g. WebUI）simultaneouslyrunningscenario。
        """
        if self._background_thread and self._background_thread.is_alive():
            logger.warning("[DingTalk Stream] clientalreadyinrunning")
            return

        self._running = True
        self._background_thread = threading.Thread(
            target=self._run_in_background,
            daemon=True,
            name="DingtalkStreamClient"
        )
        self._background_thread.start()
        logger.info("[DingTalk Stream] afterplatformclientstarted")

    def _run_in_background(self) -> None:
        """afterplatformrunning（processingabnormalandreconnect）"""
        while self._running:
            try:
                self.start()
            except Exception as e:
                logger.error(f"[DingTalk Stream] runningabnormal: {e}")
                if self._running:
                    logger.info("[DingTalk Stream] 5 secondsafterreconnect...")
                    import time
                    time.sleep(5)

    def stop(self) -> None:
        """stoppingclient"""
        self._running = False
        logger.info("[DingTalk Stream] clientstopped")

    @property
    def is_running(self) -> bool:
        """whether currentlyrunning"""
        return self._running


# globalclientinstance
_stream_client: Optional[DingtalkStreamClient] = None


def get_dingtalk_stream_client() -> Optional[DingtalkStreamClient]:
    """get global Stream clientinstance"""
    global _stream_client

    if _stream_client is None and DINGTALK_STREAM_AVAILABLE:
        try:
            _stream_client = DingtalkStreamClient()
        except (ImportError, ValueError) as e:
            logger.warning(f"[DingTalk Stream] unable tocreatingclient: {e}")
            return None

    return _stream_client


def start_dingtalk_stream_background() -> bool:
    """
    in backgroundstartDingTalk Stream client
    
    Returns:
        whethersuccessfulstart
    """
    client = get_dingtalk_stream_client()
    if client:
        client.start_background()
        return True
    return False
