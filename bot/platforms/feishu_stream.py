# -*- coding: utf-8 -*-
"""
===================================
Feishu Stream modeadapter
===================================

useFeishuofficial lark-oapi SDK  WebSocket longconnectingmodeconnectbot，
no public network needed IP and Webhook configuration。

advantage：
- no needpublic network IP ordomainname
- no needconfiguration Webhook URL
- via WebSocket longconnectingreceivingmessage
- moresimpleconnectmethod
- built-inautomaticreconnectandheartbeatkeepalive

dependency：
pip install lark-oapi

Feishulongconnectingdocument：
https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/server-side-sdk/python--sdk/handle-events
"""

import json
import logging
import threading
from datetime import datetime
from typing import Optional, Callable
import time

logger = logging.getLogger(__name__)

# tryimportFeishu SDK
try:
    import lark_oapi as lark
    from lark_oapi import ws
    from lark_oapi.api.im.v1 import (
        P2ImMessageReceiveV1,
        ReplyMessageRequest,
        ReplyMessageRequestBody,
        CreateMessageRequest,
        CreateMessageRequestBody,
    )

    FEISHU_SDK_AVAILABLE = True
except ImportError:
    FEISHU_SDK_AVAILABLE = False
    logger.warning("[Feishu Stream] lark-oapi SDK notsetup，Stream modeunavailable")
    logger.warning("[Feishu Stream] pleaserunning: pip install lark-oapi")

from bot.models import BotMessage, BotResponse, ChatType
from src.formatters import format_feishu_markdown, chunk_content_by_max_bytes
from src.config import get_config


class FeishuReplyClient:
    """
    Feishumessagereply to client
    
    useFeishu API sendingreplymessage。
    """

    def __init__(self, app_id: str, app_secret: str):
        """
        Args:
            app_id: Feishuapply ID
            app_secret: Feishuapplykey
        """
        if not FEISHU_SDK_AVAILABLE:
            raise ImportError("lark-oapi SDK notsetup")

        self._client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.WARNING) \
            .build()

        # getconfigurationmax bytes
        config = get_config()
        self._max_bytes = getattr(config, 'feishu_max_bytes', 20000)

    def _send_interactive_card(self, content: str, message_id: Optional[str] = None,
                               chat_id: Optional[str] = None,
                               receive_id_type: str = "chat_id",
                               at_user: bool = False, user_id: Optional[str] = None) -> bool:
        """
        sendinginteractive cardmessage（support Markdown render）
        
        Args:
            content: Markdown formatcontent
            message_id: originalmessage ID（replywhenuse）
            chat_id: session ID（maindynamicsendingwhenuse）
            receive_id_type: receivinger ID type
            at_user: whether @user
            user_id: user open_id（at_user=True whenneed）
            
        Returns:
            whethersendingsuccessful
        """
        try:
            # if needed @user，incontentbeforeadd @ mark
            final_content = content
            if at_user and user_id:
                final_content = f"<at user_id=\"{user_id}\"></at> {content}"
            
            # buildinteractive card payload
            card_data = {
                "config": {"wide_screen_mode": True},
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": final_content
                        }
                    }
                ]
            }

            content_json = json.dumps(card_data)

            if message_id:
                # replymessage
                request = ReplyMessageRequest.builder() \
                    .message_id(message_id) \
                    .request_body(
                    ReplyMessageRequestBody.builder()
                    .content(content_json)
                    .msg_type("interactive")
                    .build()
                ) \
                    .build()
                response = self._client.im.v1.message.reply(request)
            else:
                # maindynamicsendingmessage
                request = CreateMessageRequest.builder() \
                    .receive_id_type(receive_id_type) \
                    .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(chat_id)
                    .content(content_json)
                    .msg_type("interactive")
                    .build()
                ) \
                    .build()
                response = self._client.im.v1.message.create(request)

            if not response.success():
                logger.error(
                    f"[Feishu Stream] sendinginteractive cardfailed: code={response.code}, "
                    f"msg={response.msg}, log_id={response.get_log_id()}"
                )
                return False

            logger.debug(f"[Feishu Stream] sendinginteractive cardsuccessful")
            return True

        except Exception as e:
            logger.error(f"[Feishu Stream] sendinginteractive cardabnormal: {e}")
            return False

    def reply_text(self, message_id: str, text: str, at_user: bool = False,
                   user_id: Optional[str] = None) -> bool:
        """
        reply textmessage（support interactive cards and segmentssending）
        
        Args:
            message_id: originalmessage ID
            text: reply text
            at_user: whether @user
            user_id: user open_id（at_user=True whenneed）
            
        Returns:
            whethersendingsuccessful
        """
        # willtextconvertingasFeishu Markdown format
        formatted_text = format_feishu_markdown(text)

        # checkwhetherneedsegmentsending
        content_bytes = len(formatted_text.encode('utf-8'))
        if content_bytes > self._max_bytes:
            logger.info(
                f"[Feishu Stream] replymessagecontentextra long({content_bytes}bytes)，will batchsending"
            )
            return self._send_to_chat_chunked(
                formatted_text,
                lambda chunk: self._send_interactive_card(
                    chunk,
                    message_id=message_id,
                    at_user=at_user,
                    user_id=user_id,
                ),
            )

        # single entrymessage，useinteractive card
        return self._send_interactive_card(
            formatted_text, message_id=message_id, at_user=at_user, user_id=user_id
        )

    def send_to_chat(self, chat_id: str, text: str,
                     receive_id_type: str = "chat_id") -> bool:
        """
        sendingmessagetospecifiedsession（support interactive cards and segmentssending）
        
        Args:
            chat_id: session ID
            text: messagetext
            receive_id_type: receivinger ID type，default chat_id
            
        Returns:
            whethersendingsuccessful
        """
        # willtextconvertingasFeishu Markdown format
        formatted_text = format_feishu_markdown(text)

        # checkwhetherneedsegmentsending
        content_bytes = len(formatted_text.encode('utf-8'))
        if content_bytes > self._max_bytes:
            logger.info(
                f"[Feishu Stream] sendingmessagecontentextra long({content_bytes}bytes)，will batchsending"
            )
            return self._send_to_chat_chunked(
                formatted_text,
                lambda chunk: self._send_interactive_card(
                    chunk,
                    chat_id=chat_id,
                    receive_id_type=receive_id_type,
                ),
            )
        
        # single entrymessage，useinteractive card
        return self._send_interactive_card(formatted_text, chat_id=chat_id, receive_id_type=receive_id_type)
        
    def _send_to_chat_chunked(self, content: str, send_func: Callable[[str], bool]) -> bool:
        """
        in batchessendingmessage（support interactive cards and segmentssending）
        
        Args:
            content: messagetext
            send_func: sendingsingleminutepiecefunction，returnwhethersendingsuccessful
            
        Returns:
            whether allsendingsuccessful
        """
        chunks = chunk_content_by_max_bytes(content, self._max_bytes, add_page_marker=True)
        success_count = 0
        for i, chunk in enumerate(chunks):
            if send_func(chunk):
                success_count += 1
            else:
                logger.error(f"[Feishu Stream] sendingmessagefailed: {chunk}")
            if i < len(chunks) - 1:
                time.sleep(1)
        return success_count == len(chunks)


class FeishuStreamHandler:
    """
    Feishu Stream modemessagehandler
    
    will SDK eventconvertingto unify BotMessage format，
    and callcommanddispatcherprocessing。
    """

    def __init__(
            self,
            on_message: Callable[[BotMessage], BotResponse],
            reply_client: FeishuReplyClient
    ):
        """
        Args:
            on_message: messageprocessingpullbackfunction，receiving BotMessage return BotResponse
            reply_client: Feishureply to client
        """
        self._on_message = on_message
        self._reply_client = reply_client
        self._logger = logger

    @staticmethod
    def _truncate_log_content(text: str, max_len: int = 200) -> str:
        """truncatelogcontent"""
        cleaned = text.replace("\n", " ").strip()
        if len(cleaned) > max_len:
            return f"{cleaned[:max_len]}..."
        return cleaned

    def _log_incoming_message(self, message: BotMessage) -> None:
        """recordreceivedmessagelog"""
        content = message.raw_content or message.content or ""
        summary = self._truncate_log_content(content)
        self._logger.info(
            "[Feishu Stream] Incoming message: msg_id=%s user_id=%s "
            "chat_id=%s chat_type=%s content=%s",
            message.message_id,
            message.user_id,
            message.chat_id,
            getattr(message.chat_type, "value", message.chat_type),
            summary,
        )

    def handle_message(self, event: 'P2ImMessageReceiveV1') -> None:
        """
        processingreceivingtomessageevent
        
        Args:
            event: Feishumessagereceivingevent
        """
        try:
            # parsingmessage
            bot_message = self._parse_event_message(event)

            if bot_message is None:
                return

            self._log_incoming_message(bot_message)

            # callmessageprocessingpullback
            response = self._on_message(bot_message)

            # sendingreply
            if response and response.text:
                self._reply_client.reply_text(
                    message_id=bot_message.message_id,
                    text=response.text,
                    at_user=response.at_user,
                    user_id=bot_message.user_id if response.at_user else None
                )

        except Exception as e:
            self._logger.error(f"[Feishu Stream] processingmessagefailed: {e}")
            self._logger.exception(e)

    def _parse_event_message(self, event: 'P2ImMessageReceiveV1') -> Optional[BotMessage]:
        """
        parsingFeishueventmessageto unifyformat
        
        Args:
            event: P2ImMessageReceiveV1 eventobject
        """
        try:
            event_data = event.event
            if event_data is None:
                return None

            message_data = event_data.message
            sender_data = event_data.sender

            if message_data is None:
                return None

            # onlyprocessingtextmessage
            message_type = message_data.message_type or ""
            if message_type != "text":
                self._logger.debug(f"[Feishu Stream] ignore non-textmessage: {message_type}")
                return None

            # parsingmessagecontent
            content_str = message_data.content or "{}"
            try:
                content_json = json.loads(content_str)
                raw_content = content_json.get("text", "")
            except json.JSONDecodeError:
                raw_content = content_str

            # extractcommand（remove @bot）
            content = self._extract_command(raw_content, message_data.mentions)
            mentioned = "@" in raw_content or bool(message_data.mentions)

            # getsendingerinfo
            user_id = ""
            if sender_data and sender_data.sender_id:
                user_id = sender_data.sender_id.open_id or sender_data.sender_id.user_id or ""

            # getsessiontype
            chat_type_str = message_data.chat_type or ""
            if chat_type_str == "group":
                chat_type = ChatType.GROUP
            elif chat_type_str == "p2p":
                chat_type = ChatType.PRIVATE
            else:
                chat_type = ChatType.UNKNOWN

            # creation time
            create_time = message_data.create_time
            try:
                if create_time:
                    timestamp = datetime.fromtimestamp(int(create_time) / 1000)
                else:
                    timestamp = datetime.now()
            except (ValueError, TypeError):
                timestamp = datetime.now()

            # buildrawdata
            raw_data = {
                "header": {
                    "event_id": event.header.event_id if event.header else "",
                    "event_type": event.header.event_type if event.header else "",
                    "create_time": event.header.create_time if event.header else "",
                    "token": event.header.token if event.header else "",
                    "app_id": event.header.app_id if event.header else "",
                },
                "event": {
                    "message_id": message_data.message_id,
                    "chat_id": message_data.chat_id,
                    "chat_type": message_data.chat_type,
                    "content": message_data.content,
                }
            }

            return BotMessage(
                platform="feishu",
                message_id=message_data.message_id or "",
                user_id=user_id,
                user_name=user_id,  # Feishudo not return directlyusername
                chat_id=message_data.chat_id or "",
                chat_type=chat_type,
                content=content,
                raw_content=raw_content,
                mentioned=mentioned,
                mentions=[m.key or "" for m in (message_data.mentions or [])],
                timestamp=timestamp,
                raw_data=raw_data,
            )

        except Exception as e:
            self._logger.error(f"[Feishu Stream] parsingmessagefailed: {e}")
            return None

    def _extract_command(self, text: str, mentions: list) -> str:
        """
        extractcommandcontent（remove @bot）
        
        Feishu @user formatis：@_user_1, @_user_2 etc
        
        Args:
            text: rawmessagetext
            mentions: @mentionandlist
        """
        import re

        # method1: via mentions listremove（exactmatch）
        for mention in (mentions or []):
            key = getattr(mention, 'key', '') or ''
            if key:
                text = text.replace(key, '')

        # method2: positivethenfallback，removeFeishu @user format（@_user_N）
        # when mentions is emptyornotcorrectpasswhentake effect
        text = re.sub(r'@_user_\d+\s*', '', text)

        # cleanmultipleremainingemptystyle
        return ' '.join(text.split())


class FeishuStreamClient:
    """
    Feishu Stream modeclient
    
    encapsulation lark-oapi SDK  WebSocket client，providesimplestartAPI/interface。
    
    usage：
        client = FeishuStreamClient()
        client.start()  # blockingrunning
        
        # orerin backgroundrunning
        client.start_background()
    """

    def __init__(
            self,
            app_id: Optional[str] = None,
            app_secret: Optional[str] = None
    ):
        """
        Args:
            app_id: apply ID（if not provided, get fromconfigurationreading）
            app_secret: applykey（if not provided, get fromconfigurationreading）
        """
        if not FEISHU_SDK_AVAILABLE:
            raise ImportError(
                "lark-oapi SDK notsetup。\n"
                "pleaserunning: pip install lark-oapi"
            )

        from src.config import get_config
        config = get_config()

        self._app_id = app_id or getattr(config, 'feishu_app_id', None)
        self._app_secret = app_secret or getattr(config, 'feishu_app_secret', None)

        if not self._app_id or not self._app_secret:
            raise ValueError(
                "Feishu Stream modeneedconfiguration FEISHU_APP_ID and FEISHU_APP_SECRET"
            )

        self._ws_client: Optional[ws.Client] = None
        self._reply_client: Optional[FeishuReplyClient] = None
        self._background_thread: Optional[threading.Thread] = None
        self._running = False

    def _create_message_handler(self) -> Callable[[BotMessage], BotResponse]:
        """creatingmessageprocessingfunction"""

        def handle_message(message: BotMessage) -> BotResponse:
            from bot.dispatcher import get_dispatcher
            dispatcher = get_dispatcher()
            return dispatcher.dispatch(message)

        return handle_message

    def _create_event_handler(self) -> 'lark.EventDispatcherHandler':
        """creatingeventdispatchhandler"""
        # creatingreply to client
        self._reply_client = FeishuReplyClient(self._app_id, self._app_secret)

        # creatingmessagehandler
        handler = FeishuStreamHandler(
            self._create_message_handler(),
            self._reply_client
        )

        # creatingandregistereventhandler
        # Note：encrypt_key and verification_token inlongconnectingmodebelowis notmustneed
        # but SDK needrequestpass in（canis emptystring）
        from src.config import get_config
        config = get_config()

        encrypt_key = getattr(config, 'feishu_encrypt_key', '') or ''
        verification_token = getattr(config, 'feishu_verification_token', '') or ''

        event_handler = lark.EventDispatcherHandler.builder(
            encrypt_key=encrypt_key,
            verification_token=verification_token,
            level=lark.LogLevel.WARNING
        ).register_p2_im_message_receive_v1(
            handler.handle_message
        ).build()

        return event_handler

    def start(self) -> None:
        """
        start Stream client（blocking）
        
        thismethodwillblockingcurrentthread，directtoclientstopping。
        """
        logger.info("[Feishu Stream] starting...")

        # creatingeventhandler
        event_handler = self._create_event_handler()

        # creating WebSocket client
        self._ws_client = ws.Client(
            app_id=self._app_id,
            app_secret=self._app_secret,
            event_handler=event_handler,
            log_level=lark.LogLevel.WARNING,
            auto_reconnect=True
        )

        self._running = True
        logger.info("[Feishu Stream] clientstarted，waitingmessage...")

        # start（blocking）
        self._ws_client.start()

    def start_background(self) -> None:
        """
        in backgroundthreadstart Stream client（non-blocking）
        
        suitableforwithotherservice（e.g. WebUI）simultaneouslyrunningscenario。
        """
        if self._background_thread and self._background_thread.is_alive():
            logger.warning("[Feishu Stream] clientalreadyinrunning")
            return

        self._running = True
        self._background_thread = threading.Thread(
            target=self._run_in_background,
            daemon=True,
            name="FeishuStreamClient"
        )
        self._background_thread.start()
        logger.info("[Feishu Stream] afterplatformclientstarted")

    def _run_in_background(self) -> None:
        """afterplatformrunning（processingabnormalandreconnect）"""
        import time

        while self._running:
            try:
                self.start()
            except Exception as e:
                logger.error(f"[Feishu Stream] runningabnormal: {e}")
                if self._running:
                    logger.info("[Feishu Stream] 5 secondsafterreconnect...")
                    time.sleep(5)

    def stop(self) -> None:
        """stoppingclient"""
        self._running = False
        logger.info("[Feishu Stream] clientstopped")

    @property
    def is_running(self) -> bool:
        """whether currentlyrunning"""
        return self._running


# globalclientinstance
_stream_client: Optional[FeishuStreamClient] = None


def get_feishu_stream_client() -> Optional[FeishuStreamClient]:
    """get global Stream clientinstance"""
    global _stream_client

    if _stream_client is None and FEISHU_SDK_AVAILABLE:
        try:
            _stream_client = FeishuStreamClient()
        except (ImportError, ValueError) as e:
            logger.warning(f"[Feishu Stream] unable tocreatingclient: {e}")
            return None

    return _stream_client


def start_feishu_stream_background() -> bool:
    """
    in backgroundstartFeishu Stream client
    
    Returns:
        whethersuccessfulstart
    """
    client = get_feishu_stream_client()
    if client:
        client.start_background()
        return True
    return False
