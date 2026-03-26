# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - notificationlayer
===================================

Responsibilities:
1. summaryanalysis resultgeneratingdaily report
2. support Markdown formatoutput
3. multiplechannelpush（auto-detect）：
   - Enterprise WeChat Webhook
   - Feishu Webhook
   - Telegram Bot
   - email SMTP
   - Pushover（mobile phone/desktoppush）
"""
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum

from src.config import get_config
from src.analyzer import AnalysisResult
from src.enums import ReportType
from src.report_language import (
    get_localized_stock_name,
    get_report_labels,
    get_signal_level,
    localize_chip_health,
    localize_operation_advice,
    localize_trend_prediction,
    normalize_report_language,
)
from bot.models import BotMessage
from src.utils.data_processing import normalize_model_used
from src.notification_sender import (
    AstrbotSender,
    CustomWebhookSender,
    DiscordSender,
    EmailSender,
    FeishuSender,
    PushoverSender,
    PushplusSender,
    Serverchan3Sender,
    SlackSender,
    TelegramSender,
    WechatSender,
    WECHAT_IMAGE_MAX_BYTES
)

logger = logging.getLogger(__name__)


class NotificationChannel(Enum):
    """notification channeltype"""
    WECHAT = "wechat"      # Enterprise WeChat
    FEISHU = "feishu"      # Feishu
    TELEGRAM = "telegram"  # Telegram
    EMAIL = "email"        # email
    PUSHOVER = "pushover"  # Pushover（mobile phone/desktoppush）
    PUSHPLUS = "pushplus"  # PushPlus（domesticpushservice）
    SERVERCHAN3 = "serverchan3"  # Serversauce3（mobile phoneAPPpushservice）
    CUSTOM = "custom"      # custom Webhook
    DISCORD = "discord"    # Discord bot (Bot)
    SLACK = "slack"        # Slack
    ASTRBOT = "astrbot"
    UNKNOWN = "unknown"    # unknown


class ChannelDetector:
    """
    channeldetecthandler - simplified version
    
    based onconfigurationdirectlydeterminechanneltype（notagainneed URL parsing）
    """
    
    @staticmethod
    def get_channel_name(channel: NotificationChannel) -> str:
        """getchannelChinesename"""
        names = {
            NotificationChannel.WECHAT: "Enterprise WeChat",
            NotificationChannel.FEISHU: "Feishu",
            NotificationChannel.TELEGRAM: "Telegram",
            NotificationChannel.EMAIL: "email",
            NotificationChannel.PUSHOVER: "Pushover",
            NotificationChannel.PUSHPLUS: "PushPlus",
            NotificationChannel.SERVERCHAN3: "Serversauce3",
            NotificationChannel.CUSTOM: "customWebhook",
            NotificationChannel.DISCORD: "Discordbot",
            NotificationChannel.SLACK: "Slack",
            NotificationChannel.ASTRBOT: "ASTRBOTbot",
            NotificationChannel.UNKNOWN: "unknownchannel",
        }
        return names.get(channel, "unknownchannel")


class NotificationService(
    AstrbotSender,
    CustomWebhookSender,
    DiscordSender,
    EmailSender,
    FeishuSender,
    PushoverSender,
    PushplusSender,
    Serverchan3Sender,
    SlackSender,
    TelegramSender,
    WechatSender
):
    """
    notificationservice
    
    Responsibilities:
    1. generating Markdown formatanalyzingdaily report
    2. toallalreadyconfigurationchannelpushmessage（multiplechannelconcurrency）
    3. supportlocalsavingdaily report
    
    supportedchannel：
    - Enterprise WeChat Webhook
    - Feishu Webhook
    - Telegram Bot
    - email SMTP
    - Pushover（mobile phone/desktoppush）
    
    Note：allalreadyconfigurationchannelallwillreceivedpush
    """
    
    def __init__(self, source_message: Optional[BotMessage] = None):
        """
        initializingnotificationservice
        
        detect allconfigurationchannel，pushwhenwilltoallchannelsending
        """
        config = get_config()
        self._source_message = source_message
        self._context_channels: List[str] = []

        # Markdown convertimage（Issue #289）
        self._markdown_to_image_channels = set(
            getattr(config, 'markdown_to_image_channels', []) or []
        )
        self._markdown_to_image_max_chars = getattr(
            config, 'markdown_to_image_max_chars', 15000
        )

        # onlyanalysis resultsummary（Issue #262）：true whenonlypushsummary，notincludeindividual stockdetails
        self._report_summary_only = getattr(config, 'report_summary_only', False)
        self._history_compare_cache: Dict[Tuple[int, Tuple[Tuple[str, str], ...]], Dict[str, List[Dict[str, Any]]]] = {}

        # initializingeachchannel
        AstrbotSender.__init__(self, config)
        CustomWebhookSender.__init__(self, config)
        DiscordSender.__init__(self, config)
        EmailSender.__init__(self, config)
        FeishuSender.__init__(self, config)
        PushoverSender.__init__(self, config)
        PushplusSender.__init__(self, config)
        Serverchan3Sender.__init__(self, config)
        SlackSender.__init__(self, config)
        TelegramSender.__init__(self, config)
        WechatSender.__init__(self, config)

        # detect allconfigurationchannel
        self._available_channels = self._detect_all_channels()
        if self._has_context_channel():
            self._context_channels.append("DingTalksession")

        if not self._available_channels and not self._context_channels:
            logger.warning("notconfigurationvalidnotification channel，willnotsendingpushnotification")
        else:
            channel_names = [ChannelDetector.get_channel_name(ch) for ch in self._available_channels]
            channel_names.extend(self._context_channels)
            logger.info(f"alreadyconfiguration {len(channel_names)} countnotification channel：{', '.join(channel_names)}")

    def _normalize_report_type(self, report_type: Any) -> ReportType:
        """Normalize string/enum input into ReportType."""
        if isinstance(report_type, ReportType):
            return report_type
        return ReportType.from_str(report_type)

    def _get_report_language(self, payload: Optional[Any] = None) -> str:
        """Resolve report language from result payload or global config."""
        if isinstance(payload, list):
            for item in payload:
                language = getattr(item, "report_language", None)
                if language:
                    return normalize_report_language(language)
        elif payload is not None:
            language = getattr(payload, "report_language", None)
            if language:
                return normalize_report_language(language)

        return normalize_report_language(getattr(get_config(), "report_language", "zh"))

    def _get_labels(self, payload: Optional[Any] = None) -> Dict[str, str]:
        return get_report_labels(self._get_report_language(payload))

    def _get_display_name(self, result: AnalysisResult, language: Optional[str] = None) -> str:
        report_language = normalize_report_language(language or self._get_report_language(result))
        return self._escape_md(
            get_localized_stock_name(result.name, result.code, report_language)
        )

    def _get_history_compare_context(self, results: List[AnalysisResult]) -> Dict[str, Any]:
        """Fetch and cache history comparison data for markdown rendering."""
        config = get_config()
        history_compare_n = getattr(config, 'report_history_compare_n', 0)
        if history_compare_n <= 0 or not results:
            return {"history_by_code": {}}

        cache_key = (
            history_compare_n,
            tuple(sorted((r.code, getattr(r, 'query_id', '') or '') for r in results)),
        )
        if cache_key in self._history_compare_cache:
            return {"history_by_code": self._history_compare_cache[cache_key]}

        try:
            from src.services.history_comparison_service import get_signal_changes_batch

            exclude_ids = {
                r.code: r.query_id
                for r in results
                if getattr(r, 'query_id', None)
            }
            codes = list(dict.fromkeys(r.code for r in results))
            history_by_code = get_signal_changes_batch(
                codes,
                limit=history_compare_n,
                exclude_query_ids=exclude_ids,
            )
        except Exception as e:
            logger.debug("History comparison skipped: %s", e)
            history_by_code = {}

        self._history_compare_cache[cache_key] = history_by_code
        return {"history_by_code": history_by_code}

    def generate_aggregate_report(
        self,
        results: List[AnalysisResult],
        report_type: Any,
        report_date: Optional[str] = None,
    ) -> str:
        """Generate the aggregate report content used by merge/save/push paths."""
        normalized_type = self._normalize_report_type(report_type)
        if normalized_type == ReportType.BRIEF:
            return self.generate_brief_report(results, report_date=report_date)
        return self.generate_dashboard_report(results, report_date=report_date)

    def _collect_models_used(self, results: List[AnalysisResult]) -> List[str]:
        models: List[str] = []
        for result in results:
            model = normalize_model_used(getattr(result, "model_used", None))
            if model:
                models.append(model)
        return list(dict.fromkeys(models))
    
    def _detect_all_channels(self) -> List[NotificationChannel]:
        """
        detect allconfigurationchannel
        
        Returns:
            alreadyconfigurationchannellist
        """
        channels = []
        
        # Enterprise WeChat
        if self._wechat_url:
            channels.append(NotificationChannel.WECHAT)
        
        # Feishu
        if self._feishu_url:
            channels.append(NotificationChannel.FEISHU)
        
        # Telegram
        if self._is_telegram_configured():
            channels.append(NotificationChannel.TELEGRAM)
        
        # email
        if self._is_email_configured():
            channels.append(NotificationChannel.EMAIL)
        
        # Pushover
        if self._is_pushover_configured():
            channels.append(NotificationChannel.PUSHOVER)

        # PushPlus
        if self._pushplus_token:
            channels.append(NotificationChannel.PUSHPLUS)

       # Serversauce3
        if self._serverchan3_sendkey:
            channels.append(NotificationChannel.SERVERCHAN3)
       
        # custom Webhook
        if self._custom_webhook_urls:
            channels.append(NotificationChannel.CUSTOM)
        
        # Discord
        if self._is_discord_configured():
            channels.append(NotificationChannel.DISCORD)
        # Slack
        if self._is_slack_configured():
            channels.append(NotificationChannel.SLACK)
        # AstrBot
        if self._is_astrbot_configured():
            channels.append(NotificationChannel.ASTRBOT)
        return channels

    def is_available(self) -> bool:
        """checknotificationservicewhetheravailable（at leasthasonecountchannelorcontextchannel）"""
        return len(self._available_channels) > 0 or self._has_context_channel()
    
    def get_available_channels(self) -> List[NotificationChannel]:
        """get allalreadyconfigurationchannel"""
        return self._available_channels
    
    def get_channel_names(self) -> str:
        """get allalreadyconfigurationchannelname"""
        names = [ChannelDetector.get_channel_name(ch) for ch in self._available_channels]
        if self._has_context_channel():
            names.append("DingTalksession")
        return ', '.join(names)

    # ===== Context channel =====
    def _has_context_channel(self) -> bool:
        """determinewhetherexistsbased onmessagecontextapproachingwhenchannel（e.g.DingTalksession、Feishusession）"""
        return (
            self._extract_dingtalk_session_webhook() is not None
            or self._extract_feishu_reply_info() is not None
        )

    def _extract_dingtalk_session_webhook(self) -> Optional[str]:
        """fromsourcemessageextract fromDingTalksession Webhook（for Stream modereply）"""
        if not isinstance(self._source_message, BotMessage):
            return None
        raw_data = getattr(self._source_message, "raw_data", {}) or {}
        if not isinstance(raw_data, dict):
            return None
        session_webhook = (
            raw_data.get("_session_webhook")
            or raw_data.get("sessionWebhook")
            or raw_data.get("session_webhook")
            or raw_data.get("session_webhook_url")
        )
        if not session_webhook and isinstance(raw_data.get("headers"), dict):
            session_webhook = raw_data["headers"].get("sessionWebhook")
        return session_webhook

    def _extract_feishu_reply_info(self) -> Optional[Dict[str, str]]:
        """
        fromsourcemessageextract fromFeishureplyinfo（for Stream modereply）
        
        Returns:
            packageinclude chat_id dictionary，or None
        """
        if not isinstance(self._source_message, BotMessage):
            return None
        if getattr(self._source_message, "platform", "") != "feishu":
            return None
        chat_id = getattr(self._source_message, "chat_id", "")
        if not chat_id:
            return None
        return {"chat_id": chat_id}

    def send_to_context(self, content: str) -> bool:
        """
        tobased onmessagecontextchannelsendingmessage（for exampleDingTalk Stream session）
        
        Args:
            content: Markdown formatcontent
        """
        return self._send_via_source_context(content)
    
    def _send_via_source_context(self, content: str) -> bool:
        """
        usemessagecontext（e.g.DingTalk/Feishusession）sendingonesharesreport
        
        mainly forfrombot Stream modetriggertask，ensureresultcanreturntotriggersession。
        """
        success = False
        
        # tryDingTalksession
        session_webhook = self._extract_dingtalk_session_webhook()
        if session_webhook:
            try:
                if self._send_dingtalk_chunked(session_webhook, content, max_bytes=20000):
                    logger.info("passedDingTalksession（Stream）pushreport")
                    success = True
                else:
                    logger.error("DingTalksession（Stream）pushfailed")
            except Exception as e:
                logger.error(f"DingTalksession（Stream）pushabnormal: {e}")

        # tryFeishusession
        feishu_info = self._extract_feishu_reply_info()
        if feishu_info:
            try:
                if self._send_feishu_stream_reply(feishu_info["chat_id"], content):
                    logger.info("passedFeishusession（Stream）pushreport")
                    success = True
                else:
                    logger.error("Feishusession（Stream）pushfailed")
            except Exception as e:
                logger.error(f"Feishusession（Stream）pushabnormal: {e}")

        return success

    def _send_feishu_stream_reply(self, chat_id: str, content: str) -> bool:
        """
        viaFeishu Stream modesendingmessagetospecifiedsession
        
        Args:
            chat_id: Feishusession ID
            content: messagecontent
            
        Returns:
            whethersendingsuccessful
        """
        try:
            from bot.platforms.feishu_stream import FeishuReplyClient, FEISHU_SDK_AVAILABLE
            if not FEISHU_SDK_AVAILABLE:
                logger.warning("Feishu SDK unavailable，unable tosending Stream reply")
                return False
            
            from src.config import get_config
            config = get_config()
            
            app_id = getattr(config, 'feishu_app_id', None)
            app_secret = getattr(config, 'feishu_app_secret', None)
            
            if not app_id or not app_secret:
                logger.warning("Feishu APP_ID or APP_SECRET notconfiguration")
                return False
            
            # creatingreply to client
            reply_client = FeishuReplyClient(app_id, app_secret)
            
            # Feishutextmessagehaslengthconstraint，needin batchessending
            max_bytes = getattr(config, 'feishu_max_bytes', 20000)
            content_bytes = len(content.encode('utf-8'))
            
            if content_bytes > max_bytes:
                return self._send_feishu_stream_chunked(reply_client, chat_id, content, max_bytes)
            
            return reply_client.send_to_chat(chat_id, content)
            
        except ImportError as e:
            logger.error(f"importFeishu Stream modulefailed: {e}")
            return False
        except Exception as e:
            logger.error(f"Feishu Stream replyabnormal: {e}")
            return False

    def _send_feishu_stream_chunked(
        self, 
        reply_client, 
        chat_id: str, 
        content: str, 
        max_bytes: int
    ) -> bool:
        """
        in batchessendinglongmessagetoFeishu（Stream mode）
        
        Args:
            reply_client: FeishuReplyClient instance
            chat_id: Feishusession ID
            content: completemessagecontent
            max_bytes: single entrymessagemax bytes
            
        Returns:
            whether allsendingsuccessful
        """
        import time
        
        def get_bytes(s: str) -> int:
            return len(s.encode('utf-8'))
        
        # by paragraphorseparatelinesplitting
        if "\n---\n" in content:
            sections = content.split("\n---\n")
            separator = "\n---\n"
        elif "\n### " in content:
            parts = content.split("\n### ")
            sections = [parts[0]] + [f"### {p}" for p in parts[1:]]
            separator = "\n"
        else:
            # byrowsplitting
            sections = content.split("\n")
            separator = "\n"
        
        chunks = []
        current_chunk = []
        current_bytes = 0
        separator_bytes = get_bytes(separator)
        
        for section in sections:
            section_bytes = get_bytes(section) + separator_bytes
            
            if current_bytes + section_bytes > max_bytes:
                if current_chunk:
                    chunks.append(separator.join(current_chunk))
                current_chunk = [section]
                current_bytes = section_bytes
            else:
                current_chunk.append(section)
                current_bytes += section_bytes
        
        if current_chunk:
            chunks.append(separator.join(current_chunk))
        
        # sendingeachminuteblock
        success = True
        for i, chunk in enumerate(chunks):
            if i > 0:
                time.sleep(0.5)  # avoidrequesttoo fast
            
            if not reply_client.send_to_chat(chat_id, chunk):
                success = False
                logger.error(f"Feishu Stream minuteblock {i+1}/{len(chunks)} sendingfailed")
        
        return success
        
    def generate_daily_report(
        self,
        results: List[AnalysisResult],
        report_date: Optional[str] = None
    ) -> str:
        """
        generating Markdown formatdaily report（detailedversion）

        Args:
            results: analysis resultlist
            report_date: reportdate（defaulttoday）

        Returns:
            Markdown formatdaily reportcontent
        """
        if report_date is None:
            report_date = datetime.now().strftime('%Y-%m-%d')
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)

        # title
        report_lines = [
            f"# 📅 {report_date} {labels['report_title']}",
            "",
            f"> {labels['analyzed_prefix']} **{len(results)}** {labels['stock_unit']} | "
            f"{labels['generated_at_label']}：{datetime.now().strftime('%H:%M:%S')}",
            "",
            "---",
            "",
        ]
        
        # byscoresorting（highminuteinbefore）
        sorted_results = sorted(
            results, 
            key=lambda x: x.sentiment_score, 
            reverse=True
        )
        
        # statistics - use decision_type fieldaccurate statistics
        buy_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'buy')
        sell_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'sell')
        hold_count = sum(1 for r in results if getattr(r, 'decision_type', '') in ('hold', ''))
        avg_score = sum(r.sentiment_score for r in results) / len(results) if results else 0
        
        report_lines.extend([
            f"## 📊 {labels['summary_heading']}",
            "",
            "| indicator | value |",
            "|------|------|",
            f"| 🟢 {labels['buy_label']} | **{buy_count}** {labels['stock_unit_compact']} |",
            f"| 🟡 {labels['watch_label']} | **{hold_count}** {labels['stock_unit_compact']} |",
            f"| 🔴 {labels['sell_label']} | **{sell_count}** {labels['stock_unit_compact']} |",
            f"| 📈 {labels['avg_score_label']} | **{avg_score:.1f}** |",
            "",
            "---",
            "",
        ])
        
        # Issue #262: summary_only whenonlyoutputsummary，skipindividual stockdetails
        if self._report_summary_only:
            report_lines.extend([f"## 📊 {labels['summary_heading']}", ""])
            for r in sorted_results:
                _, emoji, _ = self._get_signal_level(r)
                report_lines.append(
                    f"{emoji} **{self._get_display_name(r, report_language)}({r.code})**: "
                    f"{localize_operation_advice(r.operation_advice, report_language)} | "
                    f"{labels['score_label']} {r.sentiment_score} | "
                    f"{localize_trend_prediction(r.trend_prediction, report_language)}"
                )
        else:
            report_lines.extend([f"## 📈 {labels['report_title']}", ""])
            # one by onestockdetailedanalyzing
            for result in sorted_results:
                _, emoji, _ = self._get_signal_level(result)
                confidence_stars = result.get_confidence_stars() if hasattr(result, 'get_confidence_stars') else '⭐⭐'
                
                report_lines.extend([
                    f"### {emoji} {self._get_display_name(result, report_language)} ({result.code})",
                    "",
                    f"**{labels['action_advice_label']}：{localize_operation_advice(result.operation_advice, report_language)}** | "
                    f"**{labels['score_label']}：{result.sentiment_score}** | "
                    f"**{labels['trend_label']}：{localize_trend_prediction(result.trend_prediction, report_language)}** | "
                    f"**Confidence：{confidence_stars}**",
                    "",
                ])

                self._append_market_snapshot(report_lines, result)
                
                # corekey point
                if hasattr(result, 'key_points') and result.key_points:
                    report_lines.extend([
                        f"**🎯 corekey point**：{result.key_points}",
                        "",
                    ])
                
                # buy/sellreasonby
                if hasattr(result, 'buy_reason') and result.buy_reason:
                    report_lines.extend([
                        f"**💡 operation reason**：{result.buy_reason}",
                        "",
                    ])
                
                # trendanalyzing
                if hasattr(result, 'trend_analysis') and result.trend_analysis:
                    report_lines.extend([
                        "#### 📉 trendanalyzing",
                        f"{result.trend_analysis}",
                        "",
                    ])
                
                # short-term/medium-termoutlook
                outlook_lines = []
                if hasattr(result, 'short_term_outlook') and result.short_term_outlook:
                    outlook_lines.append(f"- **short-term（1-3day）**：{result.short_term_outlook}")
                if hasattr(result, 'medium_term_outlook') and result.medium_term_outlook:
                    outlook_lines.append(f"- **medium-term（1-2week）**：{result.medium_term_outlook}")
                if outlook_lines:
                    report_lines.extend([
                        "#### 🔮 marketoutlook",
                        *outlook_lines,
                        "",
                    ])
                
                # technicalsanalyzing
                tech_lines = []
                if result.technical_analysis:
                    tech_lines.append(f"**composite**：{result.technical_analysis}")
                if hasattr(result, 'ma_analysis') and result.ma_analysis:
                    tech_lines.append(f"**moving average**：{result.ma_analysis}")
                if hasattr(result, 'volume_analysis') and result.volume_analysis:
                    tech_lines.append(f"**volume**：{result.volume_analysis}")
                if hasattr(result, 'pattern_analysis') and result.pattern_analysis:
                    tech_lines.append(f"**pattern**：{result.pattern_analysis}")
                if tech_lines:
                    report_lines.extend([
                        "#### 📊 technicalsanalyzing",
                        *tech_lines,
                        "",
                    ])
                
                # fundamental analysis
                fund_lines = []
                if hasattr(result, 'fundamental_analysis') and result.fundamental_analysis:
                    fund_lines.append(result.fundamental_analysis)
                if hasattr(result, 'sector_position') and result.sector_position:
                    fund_lines.append(f"**sectoradverb markerdigit**：{result.sector_position}")
                if hasattr(result, 'company_highlights') and result.company_highlights:
                    fund_lines.append(f"**companyhighlight**：{result.company_highlights}")
                if fund_lines:
                    report_lines.extend([
                        "#### 🏢 fundamental analysis",
                        *fund_lines,
                        "",
                    ])
                
                # messageaspect/sentimentaspect
                news_lines = []
                if result.news_summary:
                    news_lines.append(f"**newssummary**：{result.news_summary}")
                if hasattr(result, 'market_sentiment') and result.market_sentiment:
                    news_lines.append(f"**market sentiment**：{result.market_sentiment}")
                if hasattr(result, 'hot_topics') and result.hot_topics:
                    news_lines.append(f"**relatedhotspot**：{result.hot_topics}")
                if news_lines:
                    report_lines.extend([
                        "#### 📰 messageaspect/sentimentaspect",
                        *news_lines,
                        "",
                    ])
                
                # compositeanalyzing
                if result.analysis_summary:
                    report_lines.extend([
                        "#### 📝 compositeanalyzing",
                        result.analysis_summary,
                        "",
                    ])
                
                # riskTip
                if hasattr(result, 'risk_warning') and result.risk_warning:
                    report_lines.extend([
                        f"⚠️ **riskTip**：{result.risk_warning}",
                        "",
                    ])
                
                # datasourceDescription
                if hasattr(result, 'search_performed') and result.search_performed:
                    report_lines.append("*🔍 alreadyexecuteinternetsearch*")
                if hasattr(result, 'data_sources') and result.data_sources:
                    report_lines.append(f"*📋 Data sources:{result.data_sources}*")
                
                # error message（if exists）
                if not result.success and result.error_message:
                    report_lines.extend([
                        "",
                        f"❌ **analyzingabnormal**：{result.error_message[:100]}",
                    ])
                
                report_lines.extend([
                    "",
                    "---",
                    "",
                ])
        
        # bottominfo（removedisclaimer）
        report_lines.extend([
            "",
            f"*{labels['generated_at_label']}：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
        ])
        
        return "\n".join(report_lines)
    
    @staticmethod
    def _escape_md(name: str) -> str:
        """Escape markdown special characters in stock names (e.g. *ST → \\*ST)."""
        return name.replace('*', r'\*') if name else name

    @staticmethod
    def _clean_sniper_value(value: Any) -> str:
        """Normalize sniper point values and remove redundant label prefixes."""
        if value is None:
            return 'N/A'
        if isinstance(value, (int, float)):
            return str(value)
        if not isinstance(value, str):
            return str(value)
        if not value or value == 'N/A':
            return value
        prefixes = ['ideal buy point：', 'secondary buy point：', 'stop lossdigit：', 'target level：',
                     'ideal buy point:', 'secondary buy point:', 'stop lossdigit:', 'target level:',
                     'Ideal Entry:', 'Secondary Entry:', 'Stop Loss:', 'Target:']
        for prefix in prefixes:
            if value.startswith(prefix):
                return value[len(prefix):]
        return value

    def _get_signal_level(self, result: AnalysisResult) -> tuple:
        """Get localized signal level and color based on operation advice."""
        return get_signal_level(
            result.operation_advice,
            result.sentiment_score,
            self._get_report_language(result),
        )
    
    def generate_dashboard_report(
        self,
        results: List[AnalysisResult],
        report_date: Optional[str] = None
    ) -> str:
        """
        generatingdecisiondashboardformatdaily report（detailedversion）

        format：marketoverview + Importantinfo + coreconclusion + dataperspective + action plan

        Args:
            results: analysis resultlist
            report_date: reportdate（defaulttoday）

        Returns:
            Markdown formatdecisiondashboarddaily report
        """
        config = get_config()
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)
        reason_label = "Rationale" if report_language == "en" else "operation reason"
        risk_warning_label = "Risk Warning" if report_language == "en" else "riskTip"
        technical_heading = "Technicals" if report_language == "en" else "technicals"
        ma_label = "Moving Averages" if report_language == "en" else "moving average"
        volume_analysis_label = "Volume" if report_language == "en" else "volume"
        news_heading = "News Flow" if report_language == "en" else "messageaspect"
        if getattr(config, 'report_renderer_enabled', False) and results:
            from src.services.report_renderer import render
            out = render(
                platform='markdown',
                results=results,
                report_date=report_date,
                summary_only=self._report_summary_only,
                extra_context={
                    **self._get_history_compare_context(results),
                    "report_language": report_language,
                },
            )
            if out:
                return out

        if report_date is None:
            report_date = datetime.now().strftime('%Y-%m-%d')

        # byscoresorting（highminuteinbefore）
        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)

        # statistics - use decision_type fieldaccurate statistics
        buy_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'buy')
        sell_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'sell')
        hold_count = sum(1 for r in results if getattr(r, 'decision_type', '') in ('hold', ''))

        report_lines = [
            f"# 🎯 {report_date} {labels['dashboard_title']}",
            "",
            f"> {labels['analyzed_prefix']} **{len(results)}** {labels['stock_unit']} | "
            f"🟢{labels['buy_label']}:{buy_count} 🟡{labels['watch_label']}:{hold_count} 🔴{labels['sell_label']}:{sell_count}",
            "",
        ]

        # === add new：analysis resultsummary (Issue #112) ===
        if results:
            report_lines.extend([
                f"## 📊 {labels['summary_heading']}",
                "",
            ])
            for r in sorted_results:
                _, signal_emoji, _ = self._get_signal_level(r)
                display_name = self._get_display_name(r, report_language)
                report_lines.append(
                    f"{signal_emoji} **{display_name}({r.code})**: "
                    f"{localize_operation_advice(r.operation_advice, report_language)} | "
                    f"{labels['score_label']} {r.sentiment_score} | "
                    f"{localize_trend_prediction(r.trend_prediction, report_language)}"
                )
            report_lines.extend([
                "",
                "---",
                "",
            ])

        # one by onestockdecisiondashboard（Issue #262: summary_only whenskipdetails）
        if not self._report_summary_only:
            for result in sorted_results:
                signal_text, signal_emoji, signal_tag = self._get_signal_level(result)
                dashboard = result.dashboard if hasattr(result, 'dashboard') and result.dashboard else {}
                
                # stockname（prefer to use dashboard or result inname，convertdefinition *ST etcspecialcharacter）
                stock_name = self._get_display_name(result, report_language)
                
                report_lines.extend([
                    f"## {signal_emoji} {stock_name} ({result.code})",
                    "",
                ])
                
                # ========== public sentimentwithfundamentalsoverview（putinmostbeforeaspect）==========
                intel = dashboard.get('intelligence', {}) if dashboard else {}
                if intel:
                    report_lines.extend([
                        f"### 📰 {labels['info_heading']}",
                        "",
                    ])
                    # public sentimentsentimentsummary
                    if intel.get('sentiment_summary'):
                        report_lines.append(f"**💭 {labels['sentiment_summary_label']}**: {intel['sentiment_summary']}")
                    # performanceexpected
                    if intel.get('earnings_outlook'):
                        report_lines.append(f"**📊 {labels['earnings_outlook_label']}**: {intel['earnings_outlook']}")
                    # riskalert（prominently display）
                    risk_alerts = intel.get('risk_alerts', [])
                    if risk_alerts:
                        report_lines.append("")
                        report_lines.append(f"**🚨 {labels['risk_alerts_label']}**:")
                        for alert in risk_alerts:
                            report_lines.append(f"- {alert}")
                    # positive catalyst
                    catalysts = intel.get('positive_catalysts', [])
                    if catalysts:
                        report_lines.append("")
                        report_lines.append(f"**✨ {labels['positive_catalysts_label']}**:")
                        for cat in catalysts:
                            report_lines.append(f"- {cat}")
                    # latestmessage
                    if intel.get('latest_news'):
                        report_lines.append("")
                        report_lines.append(f"**📢 {labels['latest_news_label']}**: {intel['latest_news']}")
                    report_lines.append("")
                
                # ========== coreconclusion ==========
                core = dashboard.get('core_conclusion', {}) if dashboard else {}
                one_sentence = core.get('one_sentence', result.analysis_summary)
                time_sense = core.get('time_sensitivity', labels['default_time_sensitivity'])
                pos_advice = core.get('position_advice', {})
                
                report_lines.extend([
                    f"### 📌 {labels['core_conclusion_heading']}",
                    "",
                    f"**{signal_emoji} {signal_text}** | {localize_trend_prediction(result.trend_prediction, report_language)}",
                    "",
                    f"> **{labels['one_sentence_label']}**: {one_sentence}",
                    "",
                    f"⏰ **{labels['time_sensitivity_label']}**: {time_sense}",
                    "",
                ])
                # holdingclassificationrecommended
                if pos_advice:
                    report_lines.extend([
                        f"| {labels['position_status_label']} | {labels['action_advice_label']} |",
                        "|---------|---------|",
                        f"| 🆕 **{labels['no_position_label']}** | {pos_advice.get('no_position', localize_operation_advice(result.operation_advice, report_language))} |",
                        f"| 💼 **{labels['has_position_label']}** | {pos_advice.get('has_position', labels['continue_holding'])} |",
                        "",
                    ])

                self._append_market_snapshot(report_lines, result)
                
                # ========== dataperspective ==========
                data_persp = dashboard.get('data_perspective', {}) if dashboard else {}
                if data_persp:
                    trend_data = data_persp.get('trend_status', {})
                    price_data = data_persp.get('price_position', {})
                    vol_data = data_persp.get('volume_analysis', {})
                    chip_data = data_persp.get('chip_structure', {})
                    
                    report_lines.extend([
                        f"### 📊 {labels['data_perspective_heading']}",
                        "",
                    ])
                    # trendstatus
                    if trend_data:
                        is_bullish = (
                            f"✅ {labels['yes_label']}"
                            if trend_data.get('is_bullish', False)
                            else f"❌ {labels['no_label']}"
                        )
                        report_lines.extend([
                            f"**{labels['ma_alignment_label']}**: {trend_data.get('ma_alignment', 'N/A')} | "
                            f"{labels['bullish_alignment_label']}: {is_bullish} | "
                            f"{labels['trend_strength_label']}: {trend_data.get('trend_score', 'N/A')}/100",
                            "",
                        ])
                    # pricedigitset
                    if price_data:
                        bias_status = price_data.get('bias_status', 'N/A')
                        report_lines.extend([
                            f"| {labels['price_metrics_label']} | {labels['current_price_label']} |",
                            "|---------|------|",
                            f"| {labels['current_price_label']} | {price_data.get('current_price', 'N/A')} |",
                            f"| {labels['ma5_label']} | {price_data.get('ma5', 'N/A')} |",
                            f"| {labels['ma10_label']} | {price_data.get('ma10', 'N/A')} |",
                            f"| {labels['ma20_label']} | {price_data.get('ma20', 'N/A')} |",
                            f"| {labels['bias_ma5_label']} | {price_data.get('bias_ma5', 'N/A')}% {bias_status} |",
                            f"| {labels['support_level_label']} | {price_data.get('support_level', 'N/A')} |",
                            f"| {labels['resistance_level_label']} | {price_data.get('resistance_level', 'N/A')} |",
                            "",
                        ])
                    # volumeanalyzing
                    if vol_data:
                        report_lines.extend([
                            f"**{labels['volume_label']}**: {labels['volume_ratio_label']} {vol_data.get('volume_ratio', 'N/A')} ({vol_data.get('volume_status', '')}) | "
                            f"{labels['turnover_rate_label']} {vol_data.get('turnover_rate', 'N/A')}%",
                            f"💡 *{vol_data.get('volume_meaning', '')}*",
                            "",
                        ])
                    # chip structure
                    if chip_data:
                        chip_health = localize_chip_health(chip_data.get('chip_health', 'N/A'), report_language)
                        report_lines.extend([
                            f"**{labels['chip_label']}**: {chip_data.get('profit_ratio', 'N/A')} | {chip_data.get('avg_cost', 'N/A')} | "
                            f"{chip_data.get('concentration', 'N/A')} {chip_health}",
                            "",
                        ])
                
                # ========== action plan ==========
                battle = dashboard.get('battle_plan', {}) if dashboard else {}
                if battle:
                    report_lines.extend([
                        f"### 🎯 {labels['battle_plan_heading']}",
                        "",
                    ])
                    # sniper entry point
                    sniper = battle.get('sniper_points', {})
                    if sniper:
                        report_lines.extend([
                            f"**📍 {labels['action_points_heading']}**",
                            "",
                            f"| {labels['action_points_heading']} | {labels['current_price_label']} |",
                            "|---------|------|",
                            f"| 🎯 {labels['ideal_buy_label']} | {self._clean_sniper_value(sniper.get('ideal_buy', 'N/A'))} |",
                            f"| 🔵 {labels['secondary_buy_label']} | {self._clean_sniper_value(sniper.get('secondary_buy', 'N/A'))} |",
                            f"| 🛑 {labels['stop_loss_label']} | {self._clean_sniper_value(sniper.get('stop_loss', 'N/A'))} |",
                            f"| 🎊 {labels['take_profit_label']} | {self._clean_sniper_value(sniper.get('take_profit', 'N/A'))} |",
                            "",
                        ])
                    # positionstrategy
                    position = battle.get('position_strategy', {})
                    if position:
                        report_lines.extend([
                            f"**💰 {labels['suggested_position_label']}**: {position.get('suggested_position', 'N/A')}",
                            f"- {labels['entry_plan_label']}: {position.get('entry_plan', 'N/A')}",
                            f"- {labels['risk_control_label']}: {position.get('risk_control', 'N/A')}",
                            "",
                        ])
                    # checklist
                    checklist = battle.get('action_checklist', []) if battle else []
                    if checklist:
                        report_lines.extend([
                            f"**✅ {labels['checklist_heading']}**",
                            "",
                        ])
                        for item in checklist:
                            report_lines.append(f"- {item}")
                        report_lines.append("")
                
                # if no dashboard，displaytraditionalformat
                if not dashboard:
                    # operation reason
                    if result.buy_reason:
                        report_lines.extend([
                            f"**💡 {reason_label}**: {result.buy_reason}",
                            "",
                        ])
                    # riskTip
                    if result.risk_warning:
                        report_lines.extend([
                            f"**⚠️ {risk_warning_label}**: {result.risk_warning}",
                            "",
                        ])
                    # technicalsanalyzing
                    if result.ma_analysis or result.volume_analysis:
                        report_lines.extend([
                            f"### 📊 {technical_heading}",
                            "",
                        ])
                        if result.ma_analysis:
                            report_lines.append(f"**{ma_label}**: {result.ma_analysis}")
                        if result.volume_analysis:
                            report_lines.append(f"**{volume_analysis_label}**: {result.volume_analysis}")
                        report_lines.append("")
                    # messageaspect
                    if result.news_summary:
                        report_lines.extend([
                            f"### 📰 {news_heading}",
                            f"{result.news_summary}",
                            "",
                        ])
                
                report_lines.extend([
                    "---",
                    "",
                ])
        
        # bottom（removedisclaimer）
        report_lines.extend([
            "",
            f"*{labels['generated_at_label']}：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
        ])
        
        return "\n".join(report_lines)
    
    def generate_wechat_dashboard(self, results: List[AnalysisResult]) -> str:
        """
        generatingEnterprise WeChatdecisiondashboardsimplifiedversion（controlin4000characterin）
        
        onlykeepcoreconclusionandsniper entry point
        
        Args:
            results: analysis resultlist
            
        Returns:
            simplifiedversiondecisiondashboard
        """
        config = get_config()
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)
        if getattr(config, 'report_renderer_enabled', False) and results:
            from src.services.report_renderer import render
            out = render(
                platform='wechat',
                results=results,
                report_date=datetime.now().strftime('%Y-%m-%d'),
                summary_only=self._report_summary_only,
                extra_context={"report_language": report_language},
            )
            if out:
                return out

        report_date = datetime.now().strftime('%Y-%m-%d')
        
        # byscoresorting
        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)
        
        # statistics - use decision_type fieldaccurate statistics
        buy_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'buy')
        sell_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'sell')
        hold_count = sum(1 for r in results if getattr(r, 'decision_type', '') in ('hold', ''))
        
        lines = [
            f"## 🎯 {report_date} {labels['dashboard_title']}",
            "",
            f"> {len(results)} {labels['stock_unit']} | "
            f"🟢{labels['buy_label']}:{buy_count} 🟡{labels['watch_label']}:{hold_count} 🔴{labels['sell_label']}:{sell_count}",
            "",
        ]
        
        # Issue #262: summary_only whenonlyoutputsummarylist
        if self._report_summary_only:
            lines.append(f"**📊 {labels['summary_heading']}**")
            lines.append("")
            for r in sorted_results:
                _, signal_emoji, _ = self._get_signal_level(r)
                stock_name = self._get_display_name(r, report_language)
                lines.append(
                    f"{signal_emoji} **{stock_name}({r.code})**: "
                    f"{localize_operation_advice(r.operation_advice, report_language)} | "
                    f"{labels['score_label']} {r.sentiment_score} | "
                    f"{localize_trend_prediction(r.trend_prediction, report_language)}"
                )
        else:
            for result in sorted_results:
                signal_text, signal_emoji, _ = self._get_signal_level(result)
                dashboard = result.dashboard if hasattr(result, 'dashboard') and result.dashboard else {}
                core = dashboard.get('core_conclusion', {}) if dashboard else {}
                battle = dashboard.get('battle_plan', {}) if dashboard else {}
                intel = dashboard.get('intelligence', {}) if dashboard else {}
                
                # stockname
                stock_name = self._get_display_name(result, report_language)
                
                # titlerow：signaletclevel + stockname
                lines.append(f"### {signal_emoji} **{signal_text}** | {stock_name}({result.code})")
                lines.append("")
                
                # coredecision（one sentence）
                one_sentence = core.get('one_sentence', result.analysis_summary) if core else result.analysis_summary
                if one_sentence:
                    lines.append(f"📌 **{one_sentence[:80]}**")
                    lines.append("")
                
                # Importantinfozone（public sentiment+fundamentals）
                info_lines = []
                
                # performanceexpected
                if intel.get('earnings_outlook'):
                    outlook = str(intel['earnings_outlook'])[:60]
                    info_lines.append(f"📊 {labels['earnings_outlook_label']}: {outlook}")
                if intel.get('sentiment_summary'):
                    sentiment = str(intel['sentiment_summary'])[:50]
                    info_lines.append(f"💭 {labels['sentiment_summary_label']}: {sentiment}")
                if info_lines:
                    lines.extend(info_lines)
                    lines.append("")
                
                # riskalert（mostImportant，prominently display）
                risks = intel.get('risk_alerts', []) if intel else []
                if risks:
                    lines.append(f"🚨 **{labels['risk_alerts_label']}**:")
                    for risk in risks[:2]:  # at mostdisplay2items
                        risk_str = str(risk)
                        risk_text = risk_str[:50] + "..." if len(risk_str) > 50 else risk_str
                        lines.append(f"   • {risk_text}")
                    lines.append("")
                
                # positive catalyst
                catalysts = intel.get('positive_catalysts', []) if intel else []
                if catalysts:
                    lines.append(f"✨ **{labels['positive_catalysts_label']}**:")
                    for cat in catalysts[:2]:  # at mostdisplay2items
                        cat_str = str(cat)
                        cat_text = cat_str[:50] + "..." if len(cat_str) > 50 else cat_str
                        lines.append(f"   • {cat_text}")
                    lines.append("")
                
                # sniper entry point
                sniper = battle.get('sniper_points', {}) if battle else {}
                if sniper:
                    ideal_buy = str(sniper.get('ideal_buy', ''))
                    stop_loss = str(sniper.get('stop_loss', ''))
                    take_profit = str(sniper.get('take_profit', ''))
                    points = []
                    if ideal_buy:
                        points.append(f"🎯{labels['ideal_buy_label']}:{ideal_buy[:15]}")
                    if stop_loss:
                        points.append(f"🛑{labels['stop_loss_label']}:{stop_loss[:15]}")
                    if take_profit:
                        points.append(f"🎊{labels['take_profit_label']}:{take_profit[:15]}")
                    if points:
                        lines.append(" | ".join(points))
                        lines.append("")
                
                # holdingrecommended
                pos_advice = core.get('position_advice', {}) if core else {}
                if pos_advice:
                    no_pos = str(pos_advice.get('no_position', ''))
                    has_pos = str(pos_advice.get('has_position', ''))
                    if no_pos:
                        lines.append(f"🆕 {labels['no_position_label']}: {no_pos[:50]}")
                    if has_pos:
                        lines.append(f"💼 {labels['has_position_label']}: {has_pos[:50]}")
                    lines.append("")
                
                # checklistsimplified version
                checklist = battle.get('action_checklist', []) if battle else []
                if checklist:
                    # onlydisplaynotviaitemitem
                    failed_checks = [str(c) for c in checklist if str(c).startswith('❌') or str(c).startswith('⚠️')]
                    if failed_checks:
                        lines.append(f"**{labels['failed_checks_heading']}**:")
                        for check in failed_checks[:3]:
                            lines.append(f"   {check[:40]}")
                        lines.append("")
                
                lines.append("---")
                lines.append("")
        
        # bottom
        lines.append(f"*{labels['report_time_label']}: {datetime.now().strftime('%H:%M')}*")
        models = self._collect_models_used(results)
        if models:
            lines.append(f"*{labels['analysis_model_label']}: {', '.join(models)}*")

        content = "\n".join(lines)

        return content

    def generate_wechat_summary(self, results: List[AnalysisResult]) -> str:
        """
        generatingEnterprise WeChatsimplifiedversiondaily report（controlin4000characterin）

        Args:
            results: analysis resultlist

        Returns:
            simplifiedversion Markdown content
        """
        report_date = datetime.now().strftime('%Y-%m-%d')
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)

        # byscoresorting
        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)

        # statistics - use decision_type fieldaccurate statistics
        buy_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'buy')
        sell_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'sell')
        hold_count = sum(1 for r in results if getattr(r, 'decision_type', '') in ('hold', ''))
        avg_score = sum(r.sentiment_score for r in results) / len(results) if results else 0

        lines = [
            f"## 📅 {report_date} {labels['report_title']}",
            "",
            f"> {labels['analyzed_prefix']} **{len(results)}** {labels['stock_unit_compact']} | "
            f"🟢{labels['buy_label']}:{buy_count} 🟡{labels['watch_label']}:{hold_count} 🔴{labels['sell_label']}:{sell_count} | "
            f"{labels['avg_score_label']}:{avg_score:.0f}",
            "",
        ]
        
        # eachonlystocksimplifiedinfo（controllength）
        for result in sorted_results:
            _, emoji, _ = self._get_signal_level(result)
            
            # coreinforow
            lines.append(f"### {emoji} {self._get_display_name(result, report_language)}({result.code})")
            lines.append(
                f"**{localize_operation_advice(result.operation_advice, report_language)}** | "
                f"{labels['score_label']}:{result.sentiment_score} | "
                f"{localize_trend_prediction(result.trend_prediction, report_language)}"
            )
            
            # operation reason（truncate）
            if hasattr(result, 'buy_reason') and result.buy_reason:
                reason = result.buy_reason[:80] + "..." if len(result.buy_reason) > 80 else result.buy_reason
                lines.append(f"💡 {reason}")
            
            # corekey point
            if hasattr(result, 'key_points') and result.key_points:
                points = result.key_points[:60] + "..." if len(result.key_points) > 60 else result.key_points
                lines.append(f"🎯 {points}")
            
            # riskTip（truncate）
            if hasattr(result, 'risk_warning') and result.risk_warning:
                risk = result.risk_warning[:50] + "..." if len(result.risk_warning) > 50 else result.risk_warning
                lines.append(f"⚠️ {risk}")
            
            lines.append("")
        
        # bottom（modelrowin --- before，Issue #528）
        models = self._collect_models_used(results)
        if models:
            lines.append(f"*{labels['analysis_model_label']}: {', '.join(models)}*")
        lines.extend([
            "---",
            f"*{labels['not_investment_advice']}*",
            f"*{labels['details_report_hint']} reports/report_{report_date.replace('-', '')}.md*"
        ])

        content = "\n".join(lines)

        return content

    def generate_brief_report(
        self,
        results: List[AnalysisResult],
        report_date: Optional[str] = None,
    ) -> str:
        """
        Generate brief report (3-5 sentences per stock) for mobile/push.

        Args:
            results: Analysis results list (use [result] for single stock).
            report_date: Report date (default: today).

        Returns:
            Brief markdown content.
        """
        if report_date is None:
            report_date = datetime.now().strftime('%Y-%m-%d')
        report_language = self._get_report_language(results)
        labels = get_report_labels(report_language)
        config = get_config()
        if getattr(config, 'report_renderer_enabled', False) and results:
            from src.services.report_renderer import render
            out = render(
                platform='brief',
                results=results,
                report_date=report_date,
                summary_only=False,
                extra_context={"report_language": report_language},
            )
            if out:
                return out
        # Fallback: brief summary from dashboard report
        if not results:
            return f"# {report_date} {labels['brief_title']}\n\n{labels['no_results']}"
        sorted_results = sorted(results, key=lambda x: x.sentiment_score, reverse=True)
        buy_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'buy')
        sell_count = sum(1 for r in results if getattr(r, 'decision_type', '') == 'sell')
        hold_count = sum(1 for r in results if getattr(r, 'decision_type', '') in ('hold', ''))
        lines = [
            f"# {report_date} {labels['brief_title']}",
            "",
            f"> {len(results)} {labels['stock_unit_compact']} | 🟢{buy_count} 🟡{hold_count} 🔴{sell_count}",
            "",
        ]
        for r in sorted_results:
            _, emoji, _ = self._get_signal_level(r)
            name = self._get_display_name(r, report_language)
            dash = r.dashboard or {}
            core = dash.get('core_conclusion', {}) or {}
            one = (core.get('one_sentence') or r.analysis_summary or '')[:60]
            lines.append(
                f"**{name}({r.code})** {emoji} "
                f"{localize_operation_advice(r.operation_advice, report_language)} | "
                f"{labels['score_label']} {r.sentiment_score} | {one}"
            )
        lines.append("")
        lines.append(f"*{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        return "\n".join(lines)

    def generate_single_stock_report(self, result: AnalysisResult) -> str:
        """
        generatingsinglestockanalysis report（forsingle stockpushmode #55）
        
        formatsimplifiedbutinfocomplete，suitableeachanalyzingcomplete one stockstockimmediatelypush
        
        Args:
            result: singlestockanalysis result
            
        Returns:
            Markdown formatsingle stockreport
        """
        report_date = datetime.now().strftime('%Y-%m-%d %H:%M')
        report_language = self._get_report_language(result)
        labels = get_report_labels(report_language)
        signal_text, signal_emoji, _ = self._get_signal_level(result)
        dashboard = result.dashboard if hasattr(result, 'dashboard') and result.dashboard else {}
        core = dashboard.get('core_conclusion', {}) if dashboard else {}
        battle = dashboard.get('battle_plan', {}) if dashboard else {}
        intel = dashboard.get('intelligence', {}) if dashboard else {}
        
        # stockname（convertdefinition *ST etcspecialcharacter）
        stock_name = self._get_display_name(result, report_language)
        
        lines = [
            f"## {signal_emoji} {stock_name} ({result.code})",
            "",
            f"> {report_date} | {labels['score_label']}: **{result.sentiment_score}** | {localize_trend_prediction(result.trend_prediction, report_language)}",
            "",
        ]

        self._append_market_snapshot(lines, result)
        
        # coredecision（one sentence）
        one_sentence = core.get('one_sentence', result.analysis_summary) if core else result.analysis_summary
        if one_sentence:
            lines.extend([
                f"### 📌 {labels['core_conclusion_heading']}",
                "",
                f"**{signal_text}**: {one_sentence}",
                "",
            ])
        
        # Importantinfo（public sentiment+fundamentals）
        info_added = False
        if intel:
            if intel.get('earnings_outlook'):
                if not info_added:
                    lines.append(f"### 📰 {labels['info_heading']}")
                    lines.append("")
                    info_added = True
                lines.append(f"📊 **{labels['earnings_outlook_label']}**: {str(intel['earnings_outlook'])[:100]}")
            
            if intel.get('sentiment_summary'):
                if not info_added:
                    lines.append(f"### 📰 {labels['info_heading']}")
                    lines.append("")
                    info_added = True
                lines.append(f"💭 **{labels['sentiment_summary_label']}**: {str(intel['sentiment_summary'])[:80]}")
            
            # riskalert
            risks = intel.get('risk_alerts', [])
            if risks:
                if not info_added:
                    lines.append(f"### 📰 {labels['info_heading']}")
                    lines.append("")
                    info_added = True
                lines.append("")
                lines.append(f"🚨 **{labels['risk_alerts_label']}**:")
                for risk in risks[:3]:
                    lines.append(f"- {str(risk)[:60]}")
            
            # positive catalyst
            catalysts = intel.get('positive_catalysts', [])
            if catalysts:
                lines.append("")
                lines.append(f"✨ **{labels['positive_catalysts_label']}**:")
                for cat in catalysts[:3]:
                    lines.append(f"- {str(cat)[:60]}")
        
        if info_added:
            lines.append("")
        
        # sniper entry point
        sniper = battle.get('sniper_points', {}) if battle else {}
        if sniper:
            lines.extend([
                f"### 🎯 {labels['action_points_heading']}",
                "",
                f"| {labels['ideal_buy_label']} | {labels['stop_loss_label']} | {labels['take_profit_label']} |",
                "|------|------|------|",
            ])
            ideal_buy = sniper.get('ideal_buy', '-')
            stop_loss = sniper.get('stop_loss', '-')
            take_profit = sniper.get('take_profit', '-')
            lines.append(f"| {ideal_buy} | {stop_loss} | {take_profit} |")
            lines.append("")
        
        # holdingrecommended
        pos_advice = core.get('position_advice', {}) if core else {}
        if pos_advice:
            lines.extend([
                f"### 💼 {labels['position_advice_heading']}",
                "",
                f"- 🆕 **{labels['no_position_label']}**: {pos_advice.get('no_position', localize_operation_advice(result.operation_advice, report_language))}",
                f"- 💼 **{labels['has_position_label']}**: {pos_advice.get('has_position', labels['continue_holding'])}",
                "",
            ])
        
        lines.append("---")
        model_used = normalize_model_used(getattr(result, "model_used", None))
        if model_used:
            lines.append(f"*{labels['analysis_model_label']}: {model_used}*")
        lines.append(f"*{labels['not_investment_advice']}*")

        return "\n".join(lines)

    # Display name mapping for realtime data sources
    _SOURCE_DISPLAY_NAMES = {
        "tencent": {"zh": "Tencent Finance", "en": "Tencent Finance"},
        "akshare_em": {"zh": "Eastmoney", "en": "Eastmoney"},
        "akshare_sina": {"zh": "Sina Finance", "en": "Sina Finance"},
        "akshare_qq": {"zh": "Tencent Finance", "en": "Tencent Finance"},
        "efinance": {"zh": "Eastmoney(efinance)", "en": "Eastmoney (efinance)"},
        "tushare": {"zh": "Tushare Pro", "en": "Tushare Pro"},
        "sina": {"zh": "Sina Finance", "en": "Sina Finance"},
        "fallback": {"zh": "fallbackfallback", "en": "Fallback"},
    }

    def _get_source_display_name(self, source: Any, language: Optional[str]) -> str:
        raw_source = str(source or "N/A")
        mapping = self._SOURCE_DISPLAY_NAMES.get(raw_source)
        if not mapping:
            return raw_source
        return mapping[normalize_report_language(language)]

    def _append_market_snapshot(self, lines: List[str], result: AnalysisResult) -> None:
        snapshot = getattr(result, 'market_snapshot', None)
        if not snapshot:
            return

        report_language = self._get_report_language(result)
        labels = get_report_labels(report_language)

        lines.extend([
            f"### 📈 {labels['market_snapshot_heading']}",
            "",
            f"| {labels['close_label']} | {labels['prev_close_label']} | {labels['open_label']} | {labels['high_label']} | {labels['low_label']} | {labels['change_pct_label']} | {labels['change_amount_label']} | {labels['amplitude_label']} | {labels['volume_label']} | {labels['amount_label']} |",
            "|------|------|------|------|------|-------|-------|------|--------|--------|",
            f"| {snapshot.get('close', 'N/A')} | {snapshot.get('prev_close', 'N/A')} | "
            f"{snapshot.get('open', 'N/A')} | {snapshot.get('high', 'N/A')} | "
            f"{snapshot.get('low', 'N/A')} | {snapshot.get('pct_chg', 'N/A')} | "
            f"{snapshot.get('change_amount', 'N/A')} | {snapshot.get('amplitude', 'N/A')} | "
            f"{snapshot.get('volume', 'N/A')} | {snapshot.get('amount', 'N/A')} |",
        ])

        if "price" in snapshot:
            display_source = self._get_source_display_name(snapshot.get('source', 'N/A'), report_language)
            lines.extend([
                "",
                f"| {labels['current_price_label']} | {labels['volume_ratio_label']} | {labels['turnover_rate_label']} | {labels['source_label']} |",
                "|-------|------|--------|----------|",
                f"| {snapshot.get('price', 'N/A')} | {snapshot.get('volume_ratio', 'N/A')} | "
                f"{snapshot.get('turnover_rate', 'N/A')} | {display_source} |",
            ])

        lines.append("")

    def _should_use_image_for_channel(
        self, channel: NotificationChannel, image_bytes: Optional[bytes]
    ) -> bool:
        """
        Decide whether to send as image for the given channel (Issue #289).

        Fallback rules (send as Markdown text instead of image):
        - image_bytes is None: conversion failed / imgkit not installed / content over max_chars
        - WeChat: image exceeds ~2MB limit
        """
        if channel.value not in self._markdown_to_image_channels or image_bytes is None:
            return False
        if channel == NotificationChannel.WECHAT and len(image_bytes) > WECHAT_IMAGE_MAX_BYTES:
            logger.warning(
                "Enterprise WeChatimageover limit (%d bytes)，rollbackas Markdown textsending",
                len(image_bytes),
            )
            return False
        return True

    def send(
        self,
        content: str,
        email_stock_codes: Optional[List[str]] = None,
        email_send_to_all: bool = False
    ) -> bool:
        """
        unifiedsendingAPI/interface - toallalreadyconfigurationchannelsending

        iteratehistoryallalreadyconfigurationchannel，one by oneonesendingmessage

        Fallback rules (Markdown-to-image, Issue #289):
        - When image_bytes is None (conversion failed / imgkit not installed /
          content over max_chars): all channels configured for image will send
          as Markdown text instead.
        - When WeChat image exceeds ~2MB: that channel falls back to Markdown text.

        Args:
            content: messagecontent（Markdown format）
            email_stock_codes: stock codelist（optional，foremailchannelroutetotoshouldminutegroupemail，Issue #268）
            email_send_to_all: emailwhethersendpreviousallconfigurationemail（formarket reviewetcnostockattributioncontent）

        Returns:
            whetherat leasthasonecountchannelsendingsuccessful
        """
        context_success = self.send_to_context(content)

        if not self._available_channels:
            if context_success:
                logger.info("passedmessagecontextchannelcompletedpush（noothernotification channel）")
                return True
            logger.warning("notificationservice unavailable，skippush")
            return False

        # Markdown to image (Issue #289): convert once if any channel needs it.
        # Per-channel decision via _should_use_image_for_channel (see send() docstring for fallback rules).
        image_bytes = None
        channels_needing_image = {
            ch for ch in self._available_channels
            if ch.value in self._markdown_to_image_channels
        }
        if channels_needing_image:
            from src.md2img import markdown_to_image
            image_bytes = markdown_to_image(
                content, max_chars=self._markdown_to_image_max_chars
            )
            if image_bytes:
                logger.info("Markdown alreadyconvertingasimage，willto %s sendingimage",
                            [ch.value for ch in channels_needing_image])
            elif channels_needing_image:
                try:
                    from src.config import get_config
                    engine = getattr(get_config(), "md2img_engine", "wkhtmltoimage")
                except Exception:
                    engine = "wkhtmltoimage"
                hint = (
                    "npm i -g markdown-to-file" if engine == "markdown-to-file"
                    else "wkhtmltopdf (apt install wkhtmltopdf / brew install wkhtmltopdf)"
                )
                logger.warning(
                    "Markdown convertimagefailed，willrollbackas textsending。pleasecheck MARKDOWN_TO_IMAGE_CHANNELS configurationandsetup %s",
                    hint,
                )

        channel_names = self.get_channel_names()
        logger.info(f"currentlyto {len(self._available_channels)} countchannelsendingnotification：{channel_names}")

        success_count = 0
        fail_count = 0

        for channel in self._available_channels:
            channel_name = ChannelDetector.get_channel_name(channel)
            use_image = self._should_use_image_for_channel(channel, image_bytes)
            try:
                if channel == NotificationChannel.WECHAT:
                    if use_image:
                        result = self._send_wechat_image(image_bytes)
                    else:
                        result = self.send_to_wechat(content)
                elif channel == NotificationChannel.FEISHU:
                    result = self.send_to_feishu(content)
                elif channel == NotificationChannel.TELEGRAM:
                    if use_image:
                        result = self._send_telegram_photo(image_bytes)
                    else:
                        result = self.send_to_telegram(content)
                elif channel == NotificationChannel.EMAIL:
                    receivers = None
                    if email_send_to_all and self._stock_email_groups:
                        receivers = self.get_all_email_receivers()
                    elif email_stock_codes and self._stock_email_groups:
                        receivers = self.get_receivers_for_stocks(email_stock_codes)
                    if use_image:
                        result = self._send_email_with_inline_image(
                            image_bytes, receivers=receivers
                        )
                    else:
                        result = self.send_to_email(content, receivers=receivers)
                elif channel == NotificationChannel.PUSHOVER:
                    result = self.send_to_pushover(content)
                elif channel == NotificationChannel.PUSHPLUS:
                    result = self.send_to_pushplus(content)
                elif channel == NotificationChannel.SERVERCHAN3:
                    result = self.send_to_serverchan3(content)
                elif channel == NotificationChannel.CUSTOM:
                    if use_image:
                        result = self._send_custom_webhook_image(
                            image_bytes, fallback_content=content
                        )
                    else:
                        result = self.send_to_custom(content)
                elif channel == NotificationChannel.DISCORD:
                    result = self.send_to_discord(content)
                elif channel == NotificationChannel.SLACK:
                    if use_image:
                        result = self._send_slack_image(
                            image_bytes, fallback_content=content
                        )
                    else:
                        result = self.send_to_slack(content)
                elif channel == NotificationChannel.ASTRBOT:
                    result = self.send_to_astrbot(content)
                else:
                    logger.warning(f"unsupportednotification channel: {channel}")
                    result = False

                if result:
                    success_count += 1
                else:
                    fail_count += 1

            except Exception as e:
                logger.error(f"{channel_name} sendingfailed: {e}")
                fail_count += 1

        logger.info(f"notificationsendingcompleted：successful {success_count} count，failed {fail_count} count")
        return success_count > 0 or context_success
   
    def save_report_to_file(
        self, 
        content: str, 
        filename: Optional[str] = None
    ) -> str:
        """
        savingdaily reportto localfile
        
        Args:
            content: daily reportcontent
            filename: filename（optional，defaultby dategenerating）
            
        Returns:
            savingfilepath
        """
        from pathlib import Path
        
        if filename is None:
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"report_{date_str}.md"
        
        # ensure reports directoryexists（useitemitemrootdirectorybelow reports）
        reports_dir = Path(__file__).parent.parent / 'reports'
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        filepath = reports_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"daily reportalreadysavingto: {filepath}")
        return str(filepath)


class NotificationBuilder:
    """
    notificationmessagebuildhandler
    
    provideconvenientmessagebuildmethod
    """
    
    @staticmethod
    def build_simple_alert(
        title: str,
        content: str,
        alert_type: str = "info"
    ) -> str:
        """
        buildsimpleremindermessage
        
        Args:
            title: title
            content: content
            alert_type: type（info, warning, error, success）
        """
        emoji_map = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "success": "✅",
        }
        emoji = emoji_map.get(alert_type, "📢")
        
        return f"{emoji} **{title}**\n\n{content}"
    
    @staticmethod
    def build_stock_summary(results: List[AnalysisResult]) -> str:
        """
        buildstocksummary（simpleshortversion）
        
        suitableforfastnotification
        """
        report_language = normalize_report_language(
            next((getattr(result, "report_language", None) for result in results if getattr(result, "report_language", None)), None)
        )
        labels = get_report_labels(report_language)
        lines = [f"📊 **{labels['summary_heading']}**", ""]
        
        for r in sorted(results, key=lambda x: x.sentiment_score, reverse=True):
            _, emoji, _ = get_signal_level(r.operation_advice, r.sentiment_score, report_language)
            name = get_localized_stock_name(r.name, r.code, report_language)
            lines.append(
                f"{emoji} {name}({r.code}): {localize_operation_advice(r.operation_advice, report_language)} | "
                f"{labels['score_label']} {r.sentiment_score}"
            )
        
        return "\n".join(lines)


# convenientfunction
def get_notification_service() -> NotificationService:
    """getnotificationserviceinstance"""
    return NotificationService()


def send_daily_report(results: List[AnalysisResult]) -> bool:
    """
    sendingdailyreportshortcut
    
    auto-detectchannelandpush
    """
    service = get_notification_service()
    
    # generatingreport
    report = service.generate_daily_report(results)
    
    # savingto local
    service.save_report_to_file(report)
    
    # pushtoconfigurationchannel（auto-detect）
    return service.send(report)


if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.DEBUG)
    
    # mockanalysis result
    test_results = [
        AnalysisResult(
            code='600519',
            name='Kweichow Moutai',
            sentiment_score=75,
            trend_prediction='bullish',
            analysis_summary='technicalsstrong，messageaspectprofitgood',
            operation_advice='buy',
            technical_analysis='volume increasebreakout MA20，MACD golden cross',
            news_summary='companypublishdividendannouncement，performanceexceedexpected',
        ),
        AnalysisResult(
            code='000001',
            name='Ping An Bank',
            sentiment_score=45,
            trend_prediction='oscillation',
            analysis_summary='sidewaysconsolidation，waitingdirection',
            operation_advice='hold',
            technical_analysis='moving averageglue，trading volumeshrinkage',
            news_summary='recentperiodnomajormessage',
        ),
        AnalysisResult(
            code='300750',
            name='CATL',
            sentiment_score=35,
            trend_prediction='bearish',
            analysis_summary='technicalsweakening，Noterisk',
            operation_advice='sell',
            technical_analysis='break below MA10 support，volumeinsufficient',
            news_summary='industryintensified competition，gross marginunder pressure',
        ),
    ]
    
    service = NotificationService()
    
    # displaydetecttochannel
    print("=== notification channeldetect ===")
    print(f"currentchannel: {service.get_channel_names()}")
    print(f"channellist: {service.get_available_channels()}")
    print(f"serviceavailable: {service.is_available()}")
    
    # generatingdaily report
    print("\n=== generatingdaily reporttesting ===")
    report = service.generate_daily_report(test_results)
    print(report)
    
    # savingtofile
    print("\n=== savingdaily report ===")
    filepath = service.save_report_to_file(report)
    print(f"save successful: {filepath}")
    
    # pushtesting
    if service.is_available():
        print(f"\n=== pushtesting（{service.get_channel_names()}）===")
        success = service.send(report)
        print(f"pushresult: {'successful' if success else 'failed'}")
    else:
        print("\nnotification channelnotconfiguration，skippushtesting")
