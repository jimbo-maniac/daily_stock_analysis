# feishu_doc.py
# -*- coding: utf-8 -*-
import logging
import json
import lark_oapi as lark
from lark_oapi.api.docx.v1 import *
from typing import List, Dict, Any, Optional
from src.config import get_config

logger = logging.getLogger(__name__)


class FeishuDocManager:
    """Feishucloud documentmanager (based onofficial SDK lark-oapi)"""

    def __init__(self):
        self.config = get_config()
        self.app_id = self.config.feishu_app_id
        self.app_secret = self.config.feishu_app_secret
        self.folder_token = self.config.feishu_folder_token

        # initializing SDK client
        # SDK willautomaticprocessing tenant_access_token getandrefresh，no need forpersonIndustrialintervene
        if self.is_configured():
            self.client = lark.Client.builder() \
                .app_id(self.app_id) \
                .app_secret(self.app_secret) \
                .log_level(lark.LogLevel.INFO) \
                .build()
        else:
            self.client = None

    def is_configured(self) -> bool:
        """checkconfigurationis complete"""
        return bool(self.app_id and self.app_secret and self.folder_token)

    def create_daily_doc(self, title: str, content_md: str) -> Optional[str]:
        """
        creatingdaily reportdocument
        """
        if not self.client or not self.is_configured():
            logger.warning("Feishu SDK notinitializingorconfigurationmissing，skipcreating")
            return None

        try:
            # 1. creatingdocument
            # useofficial SDK  Builder modeconstructrequest
            create_request = CreateDocumentRequest.builder() \
                .request_body(CreateDocumentRequestBody.builder()
                              .folder_token(self.folder_token)
                              .title(title)
                              .build()) \
                .build()

            response = self.client.docx.v1.document.create(create_request)

            if not response.success():
                logger.error(f"creatingdocumentfailed: {response.code} - {response.msg} - {response.error}")
                return None

            doc_id = response.data.document.document_id
            # here's domain onlyisasgeneratinglink，actualaccesswillagainfixedto
            doc_url = f"https://feishu.cn/docx/{doc_id}"
            logger.info(f"Feishudocumentcreatingsuccessful: {title} (ID: {doc_id})")

            # 2. parsing Markdown andwritingcontent
            # will Markdown convertingas SDK need Block objectlist
            blocks = self._markdown_to_sdk_blocks(content_md)

            # Feishu API constrainteach timewriting Block quantity（recommended 50 countaround），in batcheswriting
            batch_size = 50
            doc_block_id = doc_id  # documentthisbodyalsoisonecount block

            for i in range(0, len(blocks), batch_size):
                batch_blocks = blocks[i:i + batch_size]

                # constructbatchaddblockrequest
                batch_add_request = CreateDocumentBlockChildrenRequest.builder() \
                    .document_id(doc_id) \
                    .block_id(doc_block_id) \
                    .request_body(CreateDocumentBlockChildrenRequestBody.builder()
                                  .children(batch_blocks)  # SDK need Block objectlist
                                  .index(-1)  # add moretoendtail
                                  .build()) \
                    .build()

                write_resp = self.client.docx.v1.document_block_children.create(batch_add_request)

                if not write_resp.success():
                    logger.error(f"writingdocumentcontentfailed(batch{i}): {write_resp.code} - {write_resp.msg}")

            logger.info(f"documentcontentwritingcompleted")
            return doc_url

        except Exception as e:
            logger.error(f"Feishudocumentoperationabnormal: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _markdown_to_sdk_blocks(self, md_text: str) -> List[Block]:
        """
        willsimple Markdown convertingasFeishu SDK  Block object
        """
        blocks = []
        lines = md_text.split('\n')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # defaultnormaltext (Text = 2)
            block_type = 2
            text_content = line

            # identifytitle
            if line.startswith('# '):
                block_type = 3  # H1
                text_content = line[2:]
            elif line.startswith('## '):
                block_type = 4  # H2
                text_content = line[3:]
            elif line.startswith('### '):
                block_type = 5  # H3
                text_content = line[4:]
            elif line.startswith('---'):
                # splittingline
                blocks.append(Block.builder()
                              .block_type(22)
                              .divider(Divider.builder().build())
                              .build())
                continue

            # construct Text type Block
            # SDK structurenestedcomparisondeep: Block -> Text -> elements -> TextElement -> TextRun -> content
            text_run = TextRun.builder() \
                .content(text_content) \
                .text_element_style(TextElementStyle.builder().build()) \
                .build()

            text_element = TextElement.builder() \
                .text_run(text_run) \
                .build()

            text_obj = Text.builder() \
                .elements([text_element]) \
                .style(TextStyle.builder().build()) \
                .build()

            # based on block_type put intocorrectpropertycapacityhandler
            block_builder = Block.builder().block_type(block_type)

            if block_type == 2:
                block_builder.text(text_obj)
            elif block_type == 3:
                block_builder.heading1(text_obj)
            elif block_type == 4:
                block_builder.heading2(text_obj)
            elif block_type == 5:
                block_builder.heading3(text_obj)

            blocks.append(block_builder.build())

        return blocks