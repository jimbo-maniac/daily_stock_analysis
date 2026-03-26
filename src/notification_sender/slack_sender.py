# -*- coding: utf-8 -*-
"""
Slack sendingreminderservice

Responsibilities:
1. via Slack Bot API or Incoming Webhook sending Slack message
   （simultaneouslyconfigurationwhenprefer to use Bot API，ensuretextwithimagesendingtosamechannel）
"""
import logging
import json
import requests

from src.config import Config
from src.formatters import chunk_content_by_max_bytes

logger = logging.getLogger(__name__)

# Slack Block Kit insingle section block  text fieldupper limitas 3000 character
_BLOCK_TEXT_LIMIT = 3000
# Slack chat.postMessage / Webhook  text fieldupper limitapproximately 40000 character，conservativeget 39000
_TEXT_LIMIT = 39000


class SlackSender:

    def __init__(self, config: Config):
        """
        initializing Slack configuration

        Args:
            config: configurationobject
        """
        self._slack_webhook_url = getattr(config, 'slack_webhook_url', None)
        self._slack_bot_token = getattr(config, 'slack_bot_token', None)
        self._slack_channel_id = getattr(config, 'slack_channel_id', None)
        self._webhook_verify_ssl = getattr(config, 'webhook_verify_ssl', True)

    @property
    def _use_bot(self) -> bool:
        """Bot configurationcompletewhenprioritygo Bot API，guaranteetextandimageusesametransmission channel。"""
        return bool(self._slack_bot_token and self._slack_channel_id)

    def _is_slack_configured(self) -> bool:
        """check Slack configurationis complete（support Webhook or Bot API）"""
        return self._use_bot or bool(self._slack_webhook_url)

    def send_to_slack(self, content: str) -> bool:
        """
        pushmessageto Slack（support Webhook and Bot API）

        transmissionprioritywith _send_slack_image() keep consistent：Bot > Webhook，
        avoidtextgo Webhook、imagego Bot causemessagefallenternotsamechannel。

        Args:
            content: Markdown formatmessagecontent

        Returns:
            whethersendingsuccessful
        """
        # bybytesminuteblock，avoidsingle entrymessageover limit
        try:
            chunks = chunk_content_by_max_bytes(content, _TEXT_LIMIT, add_page_marker=True)
        except Exception as e:
            logger.error(f"splitting Slack messagefailed: {e}, tryentire segmentsending。")
            chunks = [content]

        # prefer to use Bot API（with _send_slack_image keep consistent）
        if self._use_bot:
            return all(self._send_slack_bot(chunk) for chunk in chunks)

        # itstimesuse Webhook
        if self._slack_webhook_url:
            return all(self._send_slack_webhook(chunk) for chunk in chunks)

        logger.warning("Slack configurationincomplete，skippush")
        return False

    def _build_blocks(self, content: str) -> list:
        """
        willcontentbuildas Slack Block Kit format

        ifcontentexceedsingle section block constraint，willautomaticsplitminuteasmultiplecount block。
        """
        blocks = []
        # by block text upper limitsplitminute
        pos = 0
        while pos < len(content):
            segment = content[pos:pos + _BLOCK_TEXT_LIMIT]
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": segment
                }
            })
            pos += _BLOCK_TEXT_LIMIT
        return blocks

    def _send_slack_webhook(self, content: str) -> bool:
        """
        use Incoming Webhook sendingmessageto Slack

        Args:
            content: messagecontent

        Returns:
            whethersendingsuccessful
        """
        try:
            payload = {
                "text": content,
                "blocks": self._build_blocks(content),
            }
            response = requests.post(
                self._slack_webhook_url,
                data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                headers={'Content-Type': 'application/json; charset=utf-8'},
                timeout=15,
                verify=self._webhook_verify_ssl,
            )
            if response.status_code == 200 and response.text == "ok":
                logger.info("Slack Webhook messagesendingsuccessful")
                return True
            logger.error(f"Slack Webhook sendingfailed: HTTP {response.status_code} {response.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"Slack Webhook sendingabnormal: {e}")
            return False

    def _send_slack_bot(self, content: str) -> bool:
        """
        use Bot API (chat.postMessage) sendingmessageto Slack

        Args:
            content: messagecontent

        Returns:
            whethersendingsuccessful
        """
        try:
            headers = {
                'Authorization': f'Bearer {self._slack_bot_token}',
                'Content-Type': 'application/json; charset=utf-8',
            }
            payload = {
                "channel": self._slack_channel_id,
                "text": content,
                "blocks": self._build_blocks(content),
            }
            response = requests.post(
                'https://slack.com/api/chat.postMessage',
                data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                headers=headers,
                timeout=15,
            )
            result = response.json()
            if result.get("ok"):
                logger.info("Slack Bot messagesendingsuccessful")
                return True
            logger.error(f"Slack Bot sendingfailed: {result.get('error', 'unknown')}")
            return False
        except Exception as e:
            logger.error(f"Slack Bot sendingabnormal: {e}")
            return False

    def _send_slack_image(self, image_bytes: bytes, fallback_content: str = "") -> bool:
        """
        sendingimageto Slack

        Bot modebelowuse files.getUploadURLExternal + files.completeUploadExternal
        (Slack new versionfileuploading API)；Webhook modebelowrollbackas text。

        Args:
            image_bytes: PNG imagebytes
            fallback_content: imagesendingfailedwhenrollbacktext

        Returns:
            whethersendingsuccessful
        """
        # Bot mode：usenew versionfileuploading API
        if self._use_bot:
            headers = {'Authorization': f'Bearer {self._slack_bot_token}'}
            try:
                # Step 1: getuploading URL
                resp1 = requests.post(
                    'https://slack.com/api/files.getUploadURLExternal',
                    headers=headers,
                    data={
                        'filename': 'report.png',
                        'length': len(image_bytes),
                    },
                    timeout=30,
                )
                result1 = resp1.json()
                if not result1.get("ok"):
                    logger.error("Slack getuploading URL failed: %s", result1.get('error', 'unknown'))
                    raise RuntimeError(result1.get('error', 'unknown'))

                upload_url = result1['upload_url']
                file_id = result1['file_id']

                # Step 2: uploadingfilecontent（raw body，cannotuse multipart）
                resp2 = requests.post(
                    upload_url,
                    data=image_bytes,
                    headers={'Content-Type': 'application/octet-stream'},
                    timeout=30,
                )
                if resp2.status_code != 200:
                    logger.error("Slack fileuploadingfailed: HTTP %s", resp2.status_code)
                    raise RuntimeError(f"HTTP {resp2.status_code}")

                # Step 3: completeduploadingandminuteenjoytochannel
                resp3 = requests.post(
                    'https://slack.com/api/files.completeUploadExternal',
                    headers={**headers, 'Content-Type': 'application/json'},
                    json={
                        'files': [{'id': file_id, 'title': 'stockanalysis report'}],
                        'channel_id': self._slack_channel_id,
                    },
                    timeout=30,
                )
                result3 = resp3.json()
                if result3.get("ok"):
                    logger.info("Slack Bot imagesendingsuccessful")
                    return True
                logger.error("Slack completeduploadingfailed: %s", result3.get('error', 'unknown'))
            except Exception as e:
                logger.error("Slack Bot imagesendingabnormal: %s", e)

        # Webhook modeor Bot uploadingfailed：rollbackas text
        if fallback_content:
            logger.info("Slack imagenot supportedorfailed，rollbackas textsending")
            return self.send_to_slack(fallback_content)

        logger.warning("Slack imagesendingfailed，andnorollbackcontent")
        return False
