# -*- coding: utf-8 -*-
"""
===================================
batchanalyzingcommand
===================================

batchanalyzingwatchlist stockslistinallstock。
"""

import logging
import threading
import uuid
from typing import List

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse

logger = logging.getLogger(__name__)


class BatchCommand(BotCommand):
    """
    batchanalyzingcommand
    
    batchanalyzingconfigurationwatchlist stocks inlist，generatingsummaryreport。
    
    usage：
        /batch      - analyzingallwatchlist stocks
        /batch 3    - onlyanalyzingbefore3only
    """
    
    @property
    def name(self) -> str:
        return "batch"
    
    @property
    def aliases(self) -> List[str]:
        return ["b", "batch", "all"]
    
    @property
    def description(self) -> str:
        return "batchanalyzingwatchlist stocks"
    
    @property
    def usage(self) -> str:
        return "/batch [quantity]"
    
    @property
    def admin_only(self) -> bool:
        """batchanalyzingrequires adminpermission（preventabuseuse）"""
        return False  # canbased onneedset to True
    
    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        """executebatchanalyzingcommand"""
        from src.config import get_config
        
        config = get_config()
        config.refresh_stock_list()
        
        stock_list = config.stock_list
        
        if not stock_list:
            return BotResponse.error_response(
                "watchlist stockslistis empty，pleasefirstconfiguration STOCK_LIST"
            )
        
        # parsingquantityparameter
        limit = None
        if args:
            try:
                limit = int(args[0])
                if limit <= 0:
                    return BotResponse.error_response("quantitymustgreater than0")
            except ValueError:
                return BotResponse.error_response(f"invalidquantity: {args[0]}")
        
        # constraintanalyzingquantity
        if limit:
            stock_list = stock_list[:limit]
        
        logger.info(f"[BatchCommand] startingbatchanalyzing {len(stock_list)} onlystock")
        
        # in backgroundthreadinexecuteanalyzing
        thread = threading.Thread(
            target=self._run_batch_analysis,
            args=(stock_list, message),
            daemon=True
        )
        thread.start()
        
        return BotResponse.markdown_response(
            f"✅ **batchanalyzingtaskstarted**\n\n"
            f"• analyzingquantity: {len(stock_list)} only\n"
            f"• stocklist: {', '.join(stock_list[:5])}"
            f"{'...' if len(stock_list) > 5 else ''}\n\n"
            f"analyzingcompletedwill automatically afterpushsummaryreport。"
        )
    
    def _run_batch_analysis(self, stock_list: List[str], message: BotMessage) -> None:
        """afterplatformexecutebatchanalyzing"""
        try:
            from src.config import get_config
            from main import StockAnalysisPipeline
            
            config = get_config()
            
            # creatinganalyzingpipeline
            pipeline = StockAnalysisPipeline(
                config=config,
                source_message=message,
                query_id=uuid.uuid4().hex,
                query_source="bot"
            )
            
            # executeanalyzing（willautomaticpushsummaryreport）
            results = pipeline.run(
                stock_codes=stock_list,
                dry_run=False,
                send_notification=True
            )
            
            logger.info(f"[BatchCommand] batchanalyzingcompleted，successful {len(results)} only")
            
        except Exception as e:
            logger.error(f"[BatchCommand] batchanalyzingfailed: {e}")
            logger.exception(e)
