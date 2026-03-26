# -*- coding: utf-8 -*-
"""
===================================
A-share Stock Intelligent Analysis System - storage layer
===================================

Responsibilities:
1. manage SQLite databaseconnecting（singleton pattern）
2. define ORM datamodel
3. providedatastore/retrieveAPI/interface
4. implementintelligentupdatinglogic（resumable transfer）
"""

import atexit
from contextlib import contextmanager
import hashlib
import json
import logging
import re
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, TYPE_CHECKING, Tuple

import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Boolean,
    Date,
    DateTime,
    Integer,
    ForeignKey,
    Index,
    UniqueConstraint,
    Text,
    select,
    and_,
    or_,
    delete,
    desc,
    func,
)
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)
from sqlalchemy.exc import IntegrityError

from src.config import get_config

logger = logging.getLogger(__name__)

# SQLAlchemy ORM baseclass
Base = declarative_base()

if TYPE_CHECKING:
    from src.search_service import SearchResponse


# === datamodeldefine ===

class StockDaily(Base):
    """
    stockdaily linedatamodel
    
    storagedailyquote/market datadataandcalculatingtechnical indicator
    support multiplestock、multipledateunique constraint
    """
    __tablename__ = 'stock_daily'
    
    # primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # stock code（e.g. 600519, 000001）
    code = Column(String(10), nullable=False, index=True)
    
    # trading dayperiod
    date = Column(Date, nullable=False, index=True)
    
    # OHLC data
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    
    # tradedata
    volume = Column(Float)  # trading volume（stocks）
    amount = Column(Float)  # trading amount（yuan）
    pct_chg = Column(Float)  # price change percentage（%）
    
    # technical indicator
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma20 = Column(Float)
    volume_ratio = Column(Float)  # volume ratio
    
    # datasource
    data_source = Column(String(50))  # recorddatasource（e.g. AkshareFetcher）
    
    # update time
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # unique constraint：samestocksamedateonlycanhasoneitemsdata
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date'),
    )
    
    def __repr__(self):
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """convertingasdictionary"""
        return {
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }


class NewsIntel(Base):
    """
    newsintelligencedatamodel

    storagesearchtonewsintelligenceitemsitem，foraftercontinueanalyzingwithquerying
    """
    __tablename__ = 'news_intel'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # associationuserqueryingoperation
    query_id = Column(String(64), index=True)

    # stockinfo
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # searchcontext
    dimension = Column(String(32), index=True)  # latest_news / risk_check / earnings / market_analysis / industry
    query = Column(String(255))
    provider = Column(String(32), index=True)

    # newscontent
    title = Column(String(300), nullable=False)
    snippet = Column(Text)
    url = Column(String(1000), nullable=False)
    source = Column(String(100))
    published_date = Column(DateTime, index=True)

    # enterlibrarytime
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    query_source = Column(String(32), index=True)  # bot/web/cli/system
    requester_platform = Column(String(20))
    requester_user_id = Column(String(64))
    requester_user_name = Column(String(64))
    requester_chat_id = Column(String(64))
    requester_message_id = Column(String(64))
    requester_query = Column(String(255))

    __table_args__ = (
        UniqueConstraint('url', name='uix_news_url'),
        Index('ix_news_code_pub', 'code', 'published_date'),
    )

    def __repr__(self) -> str:
        return f"<NewsIntel(code={self.code}, title={self.title[:20]}...)>"


class FundamentalSnapshot(Base):
    """
    fundamentalscontextsnapshot（P0 write-only）。

    onlyforwriting，mainlinknotdependencyreadingthistable，for convenienceaftercontinuebacktest/profileextension。
    """
    __tablename__ = 'fundamental_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(String(64), nullable=False, index=True)
    code = Column(String(10), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    source_chain = Column(Text)
    coverage = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_fundamental_snapshot_query_code', 'query_id', 'code'),
        Index('ix_fundamental_snapshot_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return f"<FundamentalSnapshot(query_id={self.query_id}, code={self.code})>"


class AnalysisHistory(Base):
    """
    analysis resulthistoricalrecordmodel

    savingeach timeanalysis result，support by query_id/stock codesearch
    """
    __tablename__ = 'analysis_history'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # associationqueryinglink
    query_id = Column(String(64), index=True)

    # stockinfo
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    report_type = Column(String(16), index=True)

    # coreconclusion
    sentiment_score = Column(Integer)
    operation_advice = Column(String(20))
    trend_prediction = Column(String(50))
    analysis_summary = Column(Text)

    # detaileddata
    raw_result = Column(Text)
    news_content = Column(Text)
    context_snapshot = Column(Text)

    # sniper entry point（forbacktest）
    ideal_buy = Column(Float)
    secondary_buy = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)

    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_analysis_code_time', 'code', 'created_at'),
    )

    def to_dict(self) -> Dict[str, Any]:
        """convertingasdictionary"""
        return {
            'id': self.id,
            'query_id': self.query_id,
            'code': self.code,
            'name': self.name,
            'report_type': self.report_type,
            'sentiment_score': self.sentiment_score,
            'operation_advice': self.operation_advice,
            'trend_prediction': self.trend_prediction,
            'analysis_summary': self.analysis_summary,
            'raw_result': self.raw_result,
            'news_content': self.news_content,
            'context_snapshot': self.context_snapshot,
            'ideal_buy': self.ideal_buy,
            'secondary_buy': self.secondary_buy,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class BacktestResult(Base):
    """single entryanalyzingrecordbacktest result。"""

    __tablename__ = 'backtest_results'

    id = Column(Integer, primary_key=True, autoincrement=True)

    analysis_history_id = Column(
        Integer,
        ForeignKey('analysis_history.id'),
        nullable=False,
        index=True,
    )

    # redundantfield，for conveniencebystockfilter
    code = Column(String(10), nullable=False, index=True)
    analysis_date = Column(Date, index=True)

    # backtestparameter
    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')

    # status
    eval_status = Column(String(16), nullable=False, default='pending')
    evaluated_at = Column(DateTime, default=datetime.now, index=True)

    # recommendedsnapshot（avoidnotfromanalyzingfieldchangecausebacktestnotcanexplain）
    operation_advice = Column(String(20))
    position_recommendation = Column(String(8))  # long/cash

    # pricewithreturn/profit
    start_price = Column(Float)
    end_close = Column(Float)
    max_high = Column(Float)
    min_low = Column(Float)
    stock_return_pct = Column(Float)

    # directionwithresult
    direction_expected = Column(String(16))  # up/down/flat/not_down
    direction_correct = Column(Boolean, nullable=True)
    outcome = Column(String(16))  # win/loss/neutral

    # target pricehit（only long andconfigurationtake profit/stop losswhenhasintentdefinition）
    stop_loss = Column(Float)
    take_profit = Column(Float)
    hit_stop_loss = Column(Boolean)
    hit_take_profit = Column(Boolean)
    first_hit = Column(String(16))  # take_profit/stop_loss/ambiguous/neither/not_applicable
    first_hit_date = Column(Date)
    first_hit_trading_days = Column(Integer)

    # mockexecute（long-only）
    simulated_entry_price = Column(Float)
    simulated_exit_price = Column(Float)
    simulated_exit_reason = Column(String(24))  # stop_loss/take_profit/window_end/cash/ambiguous_stop_loss
    simulated_return_pct = Column(Float)

    __table_args__ = (
        UniqueConstraint(
            'analysis_history_id',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_analysis_window_version',
        ),
        Index('ix_backtest_code_date', 'code', 'analysis_date'),
    )


class BacktestSummary(Base):
    """backtestsummaryindicator（bystockorglobal）。"""

    __tablename__ = 'backtest_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)

    scope = Column(String(16), nullable=False, index=True)  # overall/stock
    code = Column(String(16), index=True)

    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')
    computed_at = Column(DateTime, default=datetime.now, index=True)

    # count
    total_evaluations = Column(Integer, default=0)
    completed_count = Column(Integer, default=0)
    insufficient_count = Column(Integer, default=0)
    long_count = Column(Integer, default=0)
    cash_count = Column(Integer, default=0)

    win_count = Column(Integer, default=0)
    loss_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)

    # accuraterate/win rate
    direction_accuracy_pct = Column(Float)
    win_rate_pct = Column(Float)
    neutral_rate_pct = Column(Float)

    # return/profit
    avg_stock_return_pct = Column(Float)
    avg_simulated_return_pct = Column(Float)

    # target pricetriggerstatistics（only long andconfigurationtake profit/stop losswhenstatistics）
    stop_loss_trigger_rate = Column(Float)
    take_profit_trigger_rate = Column(Float)
    ambiguous_rate = Column(Float)
    avg_days_to_first_hit = Column(Float)

    # diagnosefield（JSON string）
    advice_breakdown_json = Column(Text)
    diagnostics_json = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'scope',
            'code',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_summary_scope_code_window_version',
        ),
    )


class PortfolioAccount(Base):
    """Portfolio account metadata."""

    __tablename__ = 'portfolio_accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(String(64), index=True)
    name = Column(String(64), nullable=False)
    broker = Column(String(64))
    market = Column(String(8), nullable=False, default='cn', index=True)  # cn/hk/us
    base_currency = Column(String(8), nullable=False, default='CNY')
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index('ix_portfolio_account_owner_active', 'owner_id', 'is_active'),
    )


class PortfolioTrade(Base):
    """Executed trade events used as the source of truth for replay."""

    __tablename__ = 'portfolio_trades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    trade_uid = Column(String(128))
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    trade_date = Column(Date, nullable=False, index=True)
    side = Column(String(8), nullable=False)  # buy/sell
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fee = Column(Float, default=0.0)
    tax = Column(Float, default=0.0)
    note = Column(String(255))
    dedup_hash = Column(String(64), index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint('account_id', 'trade_uid', name='uix_portfolio_trade_uid'),
        UniqueConstraint('account_id', 'dedup_hash', name='uix_portfolio_trade_dedup_hash'),
        Index('ix_portfolio_trade_account_date', 'account_id', 'trade_date'),
    )


class PortfolioCashLedger(Base):
    """Cash in/out events."""

    __tablename__ = 'portfolio_cash_ledger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    event_date = Column(Date, nullable=False, index=True)
    direction = Column(String(8), nullable=False)  # in/out
    amount = Column(Float, nullable=False)
    currency = Column(String(8), nullable=False, default='CNY')
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_cash_account_date', 'account_id', 'event_date'),
    )


class PortfolioCorporateAction(Base):
    """Corporate actions that impact cash or share quantity."""

    __tablename__ = 'portfolio_corporate_actions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    effective_date = Column(Date, nullable=False, index=True)
    action_type = Column(String(24), nullable=False)  # cash_dividend/split_adjustment
    cash_dividend_per_share = Column(Float)
    split_ratio = Column(Float)
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_ca_account_date', 'account_id', 'effective_date'),
    )


class PortfolioPosition(Base):
    """Latest replayed position snapshot for each symbol in one account."""

    __tablename__ = 'portfolio_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    quantity = Column(Float, nullable=False, default=0.0)
    avg_cost = Column(Float, nullable=False, default=0.0)
    total_cost = Column(Float, nullable=False, default=0.0)
    last_price = Column(Float, nullable=False, default=0.0)
    market_value_base = Column(Float, nullable=False, default=0.0)
    unrealized_pnl_base = Column(Float, nullable=False, default=0.0)
    valuation_currency = Column(String(8), nullable=False, default='CNY')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'symbol',
            'market',
            'currency',
            'cost_method',
            name='uix_portfolio_position_account_symbol_market_currency',
        ),
    )


class PortfolioPositionLot(Base):
    """Lot-level remaining quantities used by FIFO replay."""

    __tablename__ = 'portfolio_position_lots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    open_date = Column(Date, nullable=False, index=True)
    remaining_quantity = Column(Float, nullable=False, default=0.0)
    unit_cost = Column(Float, nullable=False, default=0.0)
    source_trade_id = Column(Integer, ForeignKey('portfolio_trades.id'))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_lot_account_symbol', 'account_id', 'symbol'),
    )


class PortfolioDailySnapshot(Base):
    """Daily account snapshot generated by read-time replay."""

    __tablename__ = 'portfolio_daily_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')  # fifo/avg
    base_currency = Column(String(8), nullable=False, default='CNY')
    total_cash = Column(Float, nullable=False, default=0.0)
    total_market_value = Column(Float, nullable=False, default=0.0)
    total_equity = Column(Float, nullable=False, default=0.0)
    unrealized_pnl = Column(Float, nullable=False, default=0.0)
    realized_pnl = Column(Float, nullable=False, default=0.0)
    fee_total = Column(Float, nullable=False, default=0.0)
    tax_total = Column(Float, nullable=False, default=0.0)
    fx_stale = Column(Boolean, nullable=False, default=False)
    payload = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'snapshot_date',
            'cost_method',
            name='uix_portfolio_snapshot_account_date_method',
        ),
    )


class PortfolioFxRate(Base):
    """Cached FX rates used for cross-currency portfolio conversion."""

    __tablename__ = 'portfolio_fx_rates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    from_currency = Column(String(8), nullable=False, index=True)
    to_currency = Column(String(8), nullable=False, index=True)
    rate_date = Column(Date, nullable=False, index=True)
    rate = Column(Float, nullable=False)
    source = Column(String(32), nullable=False, default='manual')
    is_stale = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'from_currency',
            'to_currency',
            'rate_date',
            name='uix_portfolio_fx_pair_date',
        ),
    )


class ConversationMessage(Base):
    """
    Agent conversationhistoricalrecordtable
    """
    __tablename__ = 'conversation_messages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), index=True, nullable=False)
    role = Column(String(20), nullable=False)  # user, assistant, system
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now, index=True)


class LLMUsage(Base):
    """One row per litellm.completion() call — token-usage audit log."""

    __tablename__ = 'llm_usage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 'analysis' | 'agent' | 'market_review'
    call_type = Column(String(32), nullable=False, index=True)
    model = Column(String(128), nullable=False)
    stock_code = Column(String(16), nullable=True)
    prompt_tokens = Column(Integer, nullable=False, default=0)
    completion_tokens = Column(Integer, nullable=False, default=0)
    total_tokens = Column(Integer, nullable=False, default=0)
    called_at = Column(DateTime, default=datetime.now, index=True)


class DatabaseManager:
    """
    databasemanager - singleton pattern
    
    Responsibilities:
    1. managedatabaseconnectingpool
    2. provide Session contextmanage
    3. encapsulationdatastore/retrieveoperation
    """
    
    _instance: Optional['DatabaseManager'] = None
    _initialized: bool = False
    
    def __new__(cls, *args, **kwargs):
        """singleton patternimplement"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url: Optional[str] = None):
        """
        initializingdatabasemanager
        
        Args:
            db_url: databaseconnecting URL（optional，defaultfromconfigurationreading）
        """
        if getattr(self, '_initialized', False):
            return
        
        if db_url is None:
            config = get_config()
            db_url = config.get_db_url()
        
        # creatingdatabaseengine
        self._engine = create_engine(
            db_url,
            echo=False,  # set to True canview SQL statement
            pool_pre_ping=True,  # connectinghealthcheck
        )
        
        # creating Session factory
        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
        )
        
        # creatingalltable
        Base.metadata.create_all(self._engine)

        self._initialized = True
        logger.info(f"databaseinitializingcompleted: {db_url}")

        # registerlogouthook，ensureprogramlogoutwhenclosedatabaseconnecting
        atexit.register(DatabaseManager._cleanup_engine, self._engine)
    
    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        """getsingletoninstance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """resetsingleton（fortesting）"""
        if cls._instance is not None:
            if hasattr(cls._instance, '_engine') and cls._instance._engine is not None:
                cls._instance._engine.dispose()
            cls._instance._initialized = False
            cls._instance = None

    @classmethod
    def _cleanup_engine(cls, engine) -> None:
        """
        cleandatabaseengine（atexit hook）

        ensureprogramlogoutwhenclosealldatabaseconnecting，avoid ResourceWarning

        Args:
            engine: SQLAlchemy engineobject
        """
        try:
            if engine is not None:
                engine.dispose()
                logger.debug("databaseenginealreadyclean")
        except Exception as e:
            logger.warning(f"cleandatabaseenginewhenerror: {e}")
    
    def get_session(self) -> Session:
        """
        getdatabase Session
        
        useExample:
            with db.get_session() as session:
                # executequerying
                session.commit()  # if needed
        """
        if not getattr(self, '_initialized', False) or not hasattr(self, '_SessionLocal'):
            raise RuntimeError(
                "DatabaseManager notcorrectinitializing。"
                "pleaseensurevia DatabaseManager.get_instance() getinstance。"
            )
        session = self._SessionLocal()
        try:
            return session
        except Exception:
            session.close()
            raise

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        checkwhetherexistingspecifieddatedata
        
        forresumable transferlogic：ifexistingdatathenskipnetworkrequest
        
        Args:
            code: stock code
            target_date: target date（defaulttoday）
            
        Returns:
            whetherexistsdata
        """
        if target_date is None:
            target_date = date.today()
        # Note：here's target_date languagedefinitionis“naturalday”，andis not“latesttrading day”。
        # inweekend/holidayday/non-trading dayrunningwhen，i.e.usedatabaseexistinglatesttrading daydata，herealsowillreturn False。
        # this behavior is currently preserved（logic not changed per requirements）。
        
        with self.get_session() as session:
            result = session.execute(
                select(StockDaily).where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date == target_date
                    )
                )
            ).scalar_one_or_none()
            
            return result is not None
    
    def get_latest_data(
        self, 
        code: str, 
        days: int = 2
    ) -> List[StockDaily]:
        """
        get recent N daysdata
        
        forcalculating"compared to yesterdayday"change
        
        Args:
            code: stock code
            days: get days count
            
        Returns:
            StockDaily objectlist（by datedescending）
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()
            
            return list(results)

    def save_news_intel(
        self,
        code: str,
        name: str,
        dimension: str,
        query: str,
        response: 'SearchResponse',
        query_context: Optional[Dict[str, str]] = None
    ) -> int:
        """
        savingnewsintelligence todatabase

        deduplicatestrategy：
        - prioritize by URL deduplicate（unique constraint）
        - URL missingwhenby title + source + published_date proceedsoftdeduplicate

        associationstrategy：
        - query_context recorduserqueryinginfo（platform、user、session、rawrefers tocommandetc）
        """
        if not response or not response.results:
            return 0

        saved_count = 0
        query_ctx = query_context or {}
        current_query_id = (query_ctx.get("query_id") or "").strip()

        with self.get_session() as session:
            try:
                for item in response.results:
                    title = (item.title or '').strip()
                    url = (item.url or '').strip()
                    source = (item.source or '').strip()
                    snippet = (item.snippet or '').strip()
                    published_date = self._parse_published_date(item.published_date)

                    if not title and not url:
                        continue

                    url_key = url or self._build_fallback_url_key(
                        code=code,
                        title=title,
                        source=source,
                        published_date=published_date
                    )

                    # prioritize by URL orfallbackkeydeduplicate
                    existing = session.execute(
                        select(NewsIntel).where(NewsIntel.url == url_key)
                    ).scalar_one_or_none()

                    if existing:
                        existing.name = name or existing.name
                        existing.dimension = dimension or existing.dimension
                        existing.query = query or existing.query
                        existing.provider = response.provider or existing.provider
                        existing.snippet = snippet or existing.snippet
                        existing.source = source or existing.source
                        existing.published_date = published_date or existing.published_date
                        existing.fetched_at = datetime.now()

                        if query_context:
                            # Keep the first query_id to avoid overwriting historical links.
                            if not existing.query_id and current_query_id:
                                existing.query_id = current_query_id
                            existing.query_source = (
                                query_context.get("query_source") or existing.query_source
                            )
                            existing.requester_platform = (
                                query_context.get("requester_platform") or existing.requester_platform
                            )
                            existing.requester_user_id = (
                                query_context.get("requester_user_id") or existing.requester_user_id
                            )
                            existing.requester_user_name = (
                                query_context.get("requester_user_name") or existing.requester_user_name
                            )
                            existing.requester_chat_id = (
                                query_context.get("requester_chat_id") or existing.requester_chat_id
                            )
                            existing.requester_message_id = (
                                query_context.get("requester_message_id") or existing.requester_message_id
                            )
                            existing.requester_query = (
                                query_context.get("requester_query") or existing.requester_query
                            )
                    else:
                        try:
                            with session.begin_nested():
                                record = NewsIntel(
                                    code=code,
                                    name=name,
                                    dimension=dimension,
                                    query=query,
                                    provider=response.provider,
                                    title=title,
                                    snippet=snippet,
                                    url=url_key,
                                    source=source,
                                    published_date=published_date,
                                    fetched_at=datetime.now(),
                                    query_id=current_query_id or None,
                                    query_source=query_ctx.get("query_source"),
                                    requester_platform=query_ctx.get("requester_platform"),
                                    requester_user_id=query_ctx.get("requester_user_id"),
                                    requester_user_name=query_ctx.get("requester_user_name"),
                                    requester_chat_id=query_ctx.get("requester_chat_id"),
                                    requester_message_id=query_ctx.get("requester_message_id"),
                                    requester_query=query_ctx.get("requester_query"),
                                )
                                session.add(record)
                                session.flush()
                            saved_count += 1
                        except IntegrityError:
                            # single entry URL unique constraintconflict（e.g.concurrencyinsert），onlyskipthisitems，keepthisbatchitsremainingsuccessfulitem
                            logger.debug("newsintelligenceduplicate（skipped）: %s %s", code, url_key)

                session.commit()
                logger.info(f"savingnewsintelligencesuccessful: {code}, add new {saved_count} items")

            except Exception as e:
                session.rollback()
                logger.error(f"savingnewsintelligencefailed: {e}")
                raise

        return saved_count

    def save_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
        payload: Optional[Dict[str, Any]],
        source_chain: Optional[Any] = None,
        coverage: Optional[Any] = None,
    ) -> int:
        """
        savingfundamental snapshot（P0 write-only）。failednotsell offabnormal，returnwritingcount 0/1。
        """
        if not query_id or not code or payload is None:
            return 0

        with self.get_session() as session:
            try:
                session.add(
                    FundamentalSnapshot(
                        query_id=query_id,
                        code=code,
                        payload=self._safe_json_dumps(payload),
                        source_chain=self._safe_json_dumps(source_chain or []),
                        coverage=self._safe_json_dumps(coverage or {}),
                    )
                )
                session.commit()
                return 1
            except Exception as e:
                session.rollback()
                logger.debug(
                    "fundamental snapshotwritingfailed（fail-open）: query_id=%s code=%s err=%s",
                    query_id,
                    code,
                    e,
                )
                return 0

    def get_latest_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        get specified query_id + code latestfundamental snapshot payload。

        readingfailedornot foundreturn when None（fail-open）。
        """
        if not query_id or not code:
            return None

        with self.get_session() as session:
            try:
                row = session.execute(
                    select(FundamentalSnapshot)
                    .where(
                        and_(
                            FundamentalSnapshot.query_id == query_id,
                            FundamentalSnapshot.code == code,
                        )
                    )
                    .order_by(desc(FundamentalSnapshot.created_at))
                    .limit(1)
                ).scalar_one_or_none()
            except Exception as e:
                logger.debug(
                    "fundamental snapshotreadingfailed（fail-open）: query_id=%s code=%s err=%s",
                    query_id,
                    code,
                    e,
                )
                return None

            if row is None:
                return None
            try:
                payload = json.loads(row.payload or "{}")
                return payload if isinstance(payload, dict) else None
            except Exception:
                return None

    def get_recent_news(self, code: str, days: int = 7, limit: int = 20) -> List[NewsIntel]:
        """
        get specifiedstockrecent N daysnewsintelligence
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(
                    and_(
                        NewsIntel.code == code,
                        NewsIntel.fetched_at >= cutoff_date
                    )
                )
                .order_by(desc(NewsIntel.fetched_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_news_intel_by_query_id(self, query_id: str, limit: int = 20) -> List[NewsIntel]:
        """
        based on query_id getnewsintelligencelist

        Args:
            query_id: analyzingrecordunique identifier
            limit: return countconstraint

        Returns:
            NewsIntel list（bypublishtimeorscrapetimereverse order）
        """
        from sqlalchemy import func

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(NewsIntel.query_id == query_id)
                .order_by(
                    desc(func.coalesce(NewsIntel.published_date, NewsIntel.fetched_at)),
                    desc(NewsIntel.fetched_at)
                )
                .limit(limit)
            ).scalars().all()

            return list(results)

    def save_analysis_history(
        self,
        result: Any,
        query_id: str,
        report_type: str,
        news_content: Optional[str],
        context_snapshot: Optional[Dict[str, Any]] = None,
        save_snapshot: bool = True
    ) -> int:
        """
        savinganalysis resulthistoricalrecord
        """
        if result is None:
            return 0

        sniper_points = self._extract_sniper_points(result)
        raw_result = self._build_raw_result(result)
        context_text = None
        if save_snapshot and context_snapshot is not None:
            context_text = self._safe_json_dumps(context_snapshot)

        record = AnalysisHistory(
            query_id=query_id,
            code=result.code,
            name=result.name,
            report_type=report_type,
            sentiment_score=result.sentiment_score,
            operation_advice=result.operation_advice,
            trend_prediction=result.trend_prediction,
            analysis_summary=result.analysis_summary,
            raw_result=self._safe_json_dumps(raw_result),
            news_content=news_content,
            context_snapshot=context_text,
            ideal_buy=sniper_points.get("ideal_buy"),
            secondary_buy=sniper_points.get("secondary_buy"),
            stop_loss=sniper_points.get("stop_loss"),
            take_profit=sniper_points.get("take_profit"),
            created_at=datetime.now(),
        )

        with self.get_session() as session:
            try:
                session.add(record)
                session.commit()
                return 1
            except Exception as e:
                session.rollback()
                logger.error(f"savinganalyzinghistoricalfailed: {e}")
                return 0

    def get_analysis_history(
        self,
        code: Optional[str] = None,
        query_id: Optional[str] = None,
        days: int = 30,
        limit: int = 50,
        exclude_query_id: Optional[str] = None,
    ) -> List[AnalysisHistory]:
        """
        Query analysis history records.

        Notes:
        - If query_id is provided, perform exact lookup and ignore days window.
        - If query_id is not provided, apply days-based time filtering.
        - exclude_query_id: exclude records with this query_id (for history comparison).
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            conditions = []

            if query_id:
                conditions.append(AnalysisHistory.query_id == query_id)
            else:
                conditions.append(AnalysisHistory.created_at >= cutoff_date)

            if code:
                conditions.append(AnalysisHistory.code == code)

            # exclude_query_id only applies when not doing exact lookup (query_id is None)
            if exclude_query_id and not query_id:
                conditions.append(AnalysisHistory.query_id != exclude_query_id)

            results = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(limit)
            ).scalars().all()

            return list(results)
    
    def get_analysis_history_paginated(
        self,
        code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[AnalysisHistory], int]:
        """
        paginationqueryinganalyzinghistoricalrecord（withtotal count）
        
        Args:
            code: stock codefilter
            start_date: startingdate（include）
            end_date: end date（include）
            offset: biasedmovevolume（skipbefore N items）
            limit: items per page
            
        Returns:
            Tuple[List[AnalysisHistory], int]: (recordlist, total count)
        """
        from sqlalchemy import func
        
        with self.get_session() as session:
            conditions = []
            
            if code:
                conditions.append(AnalysisHistory.code == code)
            if start_date:
                # created_at >= start_date 00:00:00
                conditions.append(AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time()))
            if end_date:
                # created_at < end_date+1 00:00:00 (i.e. <= end_date 23:59:59)
                conditions.append(AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))
            
            # build where sub-sentence
            where_clause = and_(*conditions) if conditions else True
            
            # queryingtotal count
            total_query = select(func.count(AnalysisHistory.id)).where(where_clause)
            total = session.execute(total_query).scalar() or 0
            
            # queryingpaginationdata
            data_query = (
                select(AnalysisHistory)
                .where(where_clause)
                .order_by(desc(AnalysisHistory.created_at))
                .offset(offset)
                .limit(limit)
            )
            results = session.execute(data_query).scalars().all()
            
            return list(results), total
    
    def get_analysis_history_by_id(self, record_id: int) -> Optional[AnalysisHistory]:
        """
        based ondatabaseprimary key ID queryingsingle entryanalyzinghistoricalrecord
        
        byat query_id possiblyduplicate（batchanalyzingwhenmultipleitemsrecordtotalenjoysame query_id），
        useprimary key ID ensureexactqueryinguniquerecord。
        
        Args:
            record_id: analyzinghistoricalrecordprimary key ID
            
        Returns:
            AnalysisHistory object，not foundreturn None
        """
        with self.get_session() as session:
            result = session.execute(
                select(AnalysisHistory).where(AnalysisHistory.id == record_id)
            ).scalars().first()
            return result

    def delete_analysis_history_records(self, record_ids: List[int]) -> int:
        """
        deletingspecifiedanalyzinghistoricalrecord。

        simultaneouslycleandependencythesehistoricalrecordbacktest result，avoidforeign key constraintfailed。

        Args:
            record_ids: needdeletinghistoricalrecordprimary key ID list

        Returns:
            actualdeletinghistoricalrecordquantity
        """
        ids = sorted({int(record_id) for record_id in record_ids if record_id is not None})
        if not ids:
            return 0

        with self.session_scope() as session:
            session.execute(
                delete(BacktestResult).where(BacktestResult.analysis_history_id.in_(ids))
            )
            result = session.execute(
                delete(AnalysisHistory).where(AnalysisHistory.id.in_(ids))
            )
            return result.rowcount or 0

    def get_latest_analysis_by_query_id(self, query_id: str) -> Optional[AnalysisHistory]:
        """
        based on query_id queryinglatestoneitemsanalyzinghistoricalrecord

        query_id inbatchanalyzingwhenpossiblyduplicate，thereforereturnrecentcreatingoneitems。

        Args:
            query_id: analyzingrecordassociation query_id

        Returns:
            AnalysisHistory object，not foundreturn None
        """
        with self.get_session() as session:
            result = session.execute(
                select(AnalysisHistory)
                .where(AnalysisHistory.query_id == query_id)
                .order_by(desc(AnalysisHistory.created_at))
                .limit(1)
            ).scalars().first()
            return result
    
    def get_data_range(
        self, 
        code: str, 
        start_date: date, 
        end_date: date
    ) -> List[StockDaily]:
        """
        get specifieddaterangedata
        
        Args:
            code: stock code
            start_date: startingdate
            end_date: end date
            
        Returns:
            StockDaily objectlist
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(StockDaily.date)
            ).scalars().all()
            
            return list(results)
    
    def save_daily_data(
        self, 
        df: pd.DataFrame, 
        code: str,
        data_source: str = "Unknown"
    ) -> int:
        """
        savingdaily linedatatodatabase
        
        strategy：
        - use UPSERT logic（existsthenupdating，not foundtheninsert）
        - skipalready existsdata，avoid duplicate
        
        Args:
            df: packageincludedaily linedata DataFrame
            code: stock code
            data_source: datasourcename
            
        Returns:
            add new/updatingrecordcount
        """
        if df is None or df.empty:
            logger.warning(f"savingdata is empty，skip {code}")
            return 0
        
        saved_count = 0
        
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    # parsingdate
                    row_date = row.get('date')
                    if isinstance(row_date, str):
                        row_date = datetime.strptime(row_date, '%Y-%m-%d').date()
                    elif isinstance(row_date, datetime):
                        row_date = row_date.date()
                    elif isinstance(row_date, pd.Timestamp):
                        row_date = row_date.date()
                    
                    # checkwhetheralready exists
                    existing = session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == code,
                                StockDaily.date == row_date
                            )
                        )
                    ).scalar_one_or_none()
                    
                    if existing:
                        # updatingcurrenthasrecord
                        existing.open = row.get('open')
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source
                        existing.updated_at = datetime.now()
                    else:
                        # creatingnewrecord
                        record = StockDaily(
                            code=code,
                            date=row_date,
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            volume_ratio=row.get('volume_ratio'),
                            data_source=data_source,
                        )
                        session.add(record)
                        saved_count += 1
                
                session.commit()
                logger.info(f"saving {code} datasuccessful，add new {saved_count} items")
                
            except Exception as e:
                session.rollback()
                logger.error(f"saving {code} datafailed: {e}")
                raise
        
        return saved_count
    
    def get_analysis_context(
        self, 
        code: str,
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        getanalyzingrequiredcontextdata
        
        returntodaydata + yesterdaydaydatatocompared toinfo
        
        Args:
            code: stock code
            target_date: target date（defaulttoday）
            
        Returns:
            packageincludetodaydata、yesterdaydaytocompared toetcinfodictionary
        """
        if target_date is None:
            target_date = date.today()
        # Note：althoughenterparameterprovide target_date，butcurrentimplementactualuseis“latesttwodaysdata”（get_latest_data），
        # andnotwillby target_date exactgettoday/beforeonetrading daycontext。
        # becausethisifnotfromneedsupport“byhistoricalcertaindaysreview/recalculate”canexplain-ness，hereneedadjust。
        # this behavior is currently preserved（logic not changed per requirements）。
        
        # get recent2daysdata
        recent_data = self.get_latest_data(code, days=2)
        
        if not recent_data:
            logger.warning(f"not found {code} data")
            return None
        
        today_data = recent_data[0]
        yesterday_data = recent_data[1] if len(recent_data) > 1 else None
        
        context = {
            'code': code,
            'date': today_data.date.isoformat(),
            'today': today_data.to_dict(),
        }
        
        if yesterday_data:
            context['yesterday'] = yesterday_data.to_dict()
            
            # calculatingcompared to yesterdaydaychange
            if yesterday_data.volume and yesterday_data.volume > 0:
                context['volume_change_ratio'] = round(
                    today_data.volume / yesterday_data.volume, 2
                )
            
            if yesterday_data.close and yesterday_data.close > 0:
                context['price_change_ratio'] = round(
                    (today_data.close - yesterday_data.close) / yesterday_data.close * 100, 2
                )
            
            # moving averagepatterndetermine
            context['ma_status'] = self._analyze_ma_status(today_data)
        
        return context
    
    def _analyze_ma_status(self, data: StockDaily) -> str:
        """
        analyzingmoving averagepattern
        
        determineitemsitems：
        - long positionarrange：close > ma5 > ma10 > ma20
        - short positionarrange：close < ma5 < ma10 < ma20
        - oscillation consolidation：other cases
        """
        # Note：here'smoving averagepatterndeterminebased on“close/ma5/ma10/ma20”staticcomparison，
        # notconsidermoving averageturnpoint、sloperate、ornotsamedatasourceadjustedcaliberdifference。
        # this behavior is currently preserved（logic not changed per requirements）。
        close = data.close or 0
        ma5 = data.ma5 or 0
        ma10 = data.ma10 or 0
        ma20 = data.ma20 or 0
        
        if close > ma5 > ma10 > ma20 > 0:
            return "long positionarrange 📈"
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "short positionarrange 📉"
        elif close > ma5 and ma5 > ma10:
            return "short-termtogood 🔼"
        elif close < ma5 and ma5 < ma10:
            return "short-termweakening 🔽"
        else:
            return "oscillation consolidation ↔️"

    @staticmethod
    def _parse_published_date(value: Optional[str]) -> Optional[datetime]:
        """
        parsingpublishtimestring（failedreturn None）
        """
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        text = str(value).strip()
        if not text:
            return None

        # try first ISO format
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        return None

    @staticmethod
    def _safe_json_dumps(data: Any) -> str:
        """
        safesequencecolumn-izeas JSON string
        """
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except Exception:
            return json.dumps(str(data), ensure_ascii=False)

    @staticmethod
    def _build_raw_result(result: Any) -> Dict[str, Any]:
        """
        generatingcompleteanalysis resultdictionary
        """
        data = result.to_dict() if hasattr(result, "to_dict") else {}
        data.update({
            'data_sources': getattr(result, 'data_sources', ''),
            'raw_response': getattr(result, 'raw_response', None),
        })
        return data

    @staticmethod
    def _parse_sniper_value(value: Any) -> Optional[float]:
        """
        Parse a sniper point value from various formats to float.

        Handles: numeric types, plain number strings, Chinese price formats
        like "18.50yuan", range formats like "18.50-19.00", and text with
        embedded numbers while filtering out MA indicators.
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            v = float(value)
            return v if v > 0 else None

        text = str(value).replace(',', '').replace('，', '').strip()
        if not text or text == '-' or text == '—' or text == 'N/A':
            return None

        # trydirectlyparsingpurecountcharacterstring
        try:
            return float(text)
        except ValueError:
            pass

        # prioritytruncate "：" to "yuan" betweenprice，avoiderrorextract MA5/MA10 etctechnical indicatorcountcharacter
        colon_pos = max(text.rfind("："), text.rfind(":"))
        yuan_pos = text.find("yuan", colon_pos + 1 if colon_pos != -1 else 0)
        if yuan_pos != -1:
            segment_start = colon_pos + 1 if colon_pos != -1 else 0
            segment = text[segment_start:yuan_pos]
            
            # use finditer andfilteringdrop MA startcountcharacter
            matches = list(re.finditer(r"-?\d+(?:\.\d+)?", segment))
            valid_numbers = []
            for m in matches:
                # checkbeforeaspectwhether is "MA" (ignoresizewrite)
                start_idx = m.start()
                if start_idx >= 2:
                    prefix = segment[start_idx-2:start_idx].upper()
                    if prefix == "MA":
                        continue
                valid_numbers.append(m.group())
            
            if valid_numbers:
                try:
                    return abs(float(valid_numbers[-1]))
                except ValueError:
                    pass

        # fallback：no"yuan"characterwhen，firstinterceptremovetheonecountbracketnumberaftercontent，avoiderrorextractbracketwithin numbertechnical indicatorcountcharacter
        # for example "1.52-1.53 (pullbackMA5/10near)" → only in "1.52-1.53 " insearch
        paren_pos = len(text)
        for paren_char in ('(', '（'):
            pos = text.find(paren_char)
            if pos != -1:
                paren_pos = min(paren_pos, pos)
        search_text = text[:paren_pos].strip() or text  # bracketnumberbeforeis emptywhenfallbackusealltext

        valid_numbers = []
        for m in re.finditer(r"\d+(?:\.\d+)?", search_text):
            start_idx = m.start()
            if start_idx >= 2 and search_text[start_idx-2:start_idx].upper() == "MA":
                continue
            valid_numbers.append(m.group())
        if valid_numbers:
            try:
                return float(valid_numbers[-1])
            except ValueError:
                pass
        return None

    def _extract_sniper_points(self, result: Any) -> Dict[str, Optional[float]]:
        """
        Extract sniper point values from an AnalysisResult.

        Tries multiple extraction paths to handle different dashboard structures:
        1. result.get_sniper_points() (standard path)
        2. Direct dashboard dict traversal with various nesting levels
        3. Fallback from raw_result dict if available
        """
        raw_points = {}

        # Path 1: standard method
        if hasattr(result, "get_sniper_points"):
            raw_points = result.get_sniper_points() or {}

        # Path 2: direct dashboard traversal when standard path yields empty values
        if not any(raw_points.get(k) for k in ("ideal_buy", "secondary_buy", "stop_loss", "take_profit")):
            dashboard = getattr(result, "dashboard", None)
            if isinstance(dashboard, dict):
                raw_points = self._find_sniper_in_dashboard(dashboard) or raw_points

        # Path 3: try raw_result for agent mode results
        if not any(raw_points.get(k) for k in ("ideal_buy", "secondary_buy", "stop_loss", "take_profit")):
            raw_response = getattr(result, "raw_response", None)
            if isinstance(raw_response, dict):
                raw_points = self._find_sniper_in_dashboard(raw_response) or raw_points

        return {
            "ideal_buy": self._parse_sniper_value(raw_points.get("ideal_buy")),
            "secondary_buy": self._parse_sniper_value(raw_points.get("secondary_buy")),
            "stop_loss": self._parse_sniper_value(raw_points.get("stop_loss")),
            "take_profit": self._parse_sniper_value(raw_points.get("take_profit")),
        }

    @staticmethod
    def _find_sniper_in_dashboard(d: dict) -> Optional[Dict[str, Any]]:
        """
        Recursively search for sniper_points in a dashboard dict.
        Handles various nesting: dashboard.battle_plan.sniper_points,
        dashboard.dashboard.battle_plan.sniper_points, etc.
        """
        if not isinstance(d, dict):
            return None

        # Direct: d has sniper_points keys at top level
        if "ideal_buy" in d:
            return d

        # d.sniper_points
        sp = d.get("sniper_points")
        if isinstance(sp, dict) and sp:
            return sp

        # d.battle_plan.sniper_points
        bp = d.get("battle_plan")
        if isinstance(bp, dict):
            sp = bp.get("sniper_points")
            if isinstance(sp, dict) and sp:
                return sp

        # d.dashboard.battle_plan.sniper_points (double-nested)
        inner = d.get("dashboard")
        if isinstance(inner, dict):
            bp = inner.get("battle_plan")
            if isinstance(bp, dict):
                sp = bp.get("sniper_points")
                if isinstance(sp, dict) and sp:
                    return sp

        return None

    @staticmethod
    def _build_fallback_url_key(
        code: str,
        title: str,
        source: str,
        published_date: Optional[datetime]
    ) -> str:
        """
        generatingno URL whendeduplicatekey（ensurestableandrelativelyshort）
        """
        date_str = published_date.isoformat() if published_date else ""
        raw_key = f"{code}|{title}|{source}|{date_str}"
        digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()
        return f"no-url:{code}:{digest}"

    def save_conversation_message(self, session_id: str, role: str, content: str) -> None:
        """
        saving Agent conversationmessage
        """
        with self.session_scope() as session:
            msg = ConversationMessage(
                session_id=session_id,
                role=role,
                content=content
            )
            session.add(msg)

    def get_conversation_history(self, session_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        get Agent conversationhistorical
        """
        with self.session_scope() as session:
            stmt = select(ConversationMessage).filter(
                ConversationMessage.session_id == session_id
            ).order_by(ConversationMessage.created_at.desc()).limit(limit)
            messages = session.execute(stmt).scalars().all()

            # reverse orderreturn，guaranteetimeorder
            return [{"role": msg.role, "content": msg.content} for msg in reversed(messages)]

    def conversation_session_exists(self, session_id: str) -> bool:
        """Return True when at least one message exists for the given session."""
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage.id)
                .where(ConversationMessage.session_id == session_id)
                .limit(1)
            )
            return session.execute(stmt).scalar() is not None

    def get_chat_sessions(
        self,
        limit: int = 50,
        session_prefix: Optional[str] = None,
        extra_session_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        getchatdayssessionlist（from conversation_messages aggregation）

        Args:
            limit: Maximum number of sessions to return.
            session_prefix: If provided, only return sessions whose session_id
                starts with this prefix.  Used for per-user isolation (e.g.
                ``"telegram_12345"``).
            extra_session_ids: Optional exact session ids to include in
                addition to the scoped prefix.

        Returns:
            byrecentactivetimereverse ordersessionlist，eachitemspackageinclude session_id, title, message_count, last_active
        """
        from sqlalchemy import func

        with self.session_scope() as session:
            normalized_prefix = None
            if session_prefix:
                normalized_prefix = session_prefix if session_prefix.endswith(":") else f"{session_prefix}:"
            exact_ids = [sid for sid in (extra_session_ids or []) if sid]

            # aggregationeach session messagecountandmostafteractivetime
            base = (
                select(
                    ConversationMessage.session_id,
                    func.count(ConversationMessage.id).label("message_count"),
                    func.min(ConversationMessage.created_at).label("created_at"),
                    func.max(ConversationMessage.created_at).label("last_active"),
                )
            )
            conditions = []
            if normalized_prefix:
                conditions.append(ConversationMessage.session_id.startswith(normalized_prefix))
            if exact_ids:
                conditions.append(ConversationMessage.session_id.in_(exact_ids))
            if conditions:
                base = base.where(or_(*conditions))
            stmt = (
                base
                .group_by(ConversationMessage.session_id)
                .order_by(desc(func.max(ConversationMessage.created_at)))
                .limit(limit)
            )
            rows = session.execute(stmt).all()

            results = []
            for row in rows:
                sid = row.session_id
                # getthissessiontheoneitems user messageact asastitle
                first_user_msg = session.execute(
                    select(ConversationMessage.content)
                    .where(
                        and_(
                            ConversationMessage.session_id == sid,
                            ConversationMessage.role == "user",
                        )
                    )
                    .order_by(ConversationMessage.created_at)
                    .limit(1)
                ).scalar()
                title = (first_user_msg or "newconversation")[:60]

                results.append({
                    "session_id": sid,
                    "title": title,
                    "message_count": row.message_count,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "last_active": row.last_active.isoformat() if row.last_active else None,
                })
            return results

    def get_conversation_messages(self, session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        getsinglesessioncompletemessagelist（forfrontendrestorehistorical）
        """
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage)
                .where(ConversationMessage.session_id == session_id)
                .order_by(ConversationMessage.created_at)
                .limit(limit)
            )
            messages = session.execute(stmt).scalars().all()
            return [
                {
                    "id": str(msg.id),
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at.isoformat() if msg.created_at else None,
                }
                for msg in messages
            ]

    def delete_conversation_session(self, session_id: str) -> int:
        """
        deletingspecifiedsessionallmessage

        Returns:
            deletingmessagecount
        """
        with self.session_scope() as session:
            result = session.execute(
                delete(ConversationMessage).where(
                    ConversationMessage.session_id == session_id
                )
            )
            return result.rowcount

    # ------------------------------------------------------------------
    # LLM usage tracking
    # ------------------------------------------------------------------

    def record_llm_usage(
        self,
        call_type: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        stock_code: Optional[str] = None,
    ) -> None:
        """Append one LLM call record to llm_usage."""
        row = LLMUsage(
            call_type=call_type,
            model=model or "unknown",
            stock_code=stock_code,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )
        with self.session_scope() as session:
            session.add(row)

    def get_llm_usage_summary(
        self,
        from_dt: datetime,
        to_dt: datetime,
    ) -> Dict[str, Any]:
        """Return aggregated token usage between from_dt and to_dt.

        Returns a dict with keys:
          total_calls, total_tokens,
          by_call_type: list of {call_type, calls, total_tokens},
          by_model:     list of {model, calls, total_tokens}
        """
        with self.session_scope() as session:
            base_filter = and_(
                LLMUsage.called_at >= from_dt,
                LLMUsage.called_at <= to_dt,
            )

            # Overall totals
            totals = session.execute(
                select(
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                ).where(base_filter)
            ).one()

            # Breakdown by call_type
            by_type_rows = session.execute(
                select(
                    LLMUsage.call_type,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.call_type)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

            # Breakdown by model
            by_model_rows = session.execute(
                select(
                    LLMUsage.model,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.model)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

        return {
            "total_calls": totals.calls,
            "total_tokens": totals.tokens,
            "by_call_type": [
                {"call_type": r.call_type, "calls": r.calls, "total_tokens": r.tokens}
                for r in by_type_rows
            ],
            "by_model": [
                {"model": r.model, "calls": r.calls, "total_tokens": r.tokens}
                for r in by_model_rows
            ],
        }


# convenientfunction
def get_db() -> DatabaseManager:
    """getdatabasemanagerinstanceshortcut"""
    return DatabaseManager.get_instance()


def persist_llm_usage(
    usage: Dict[str, Any],
    model: str,
    call_type: str,
    stock_code: Optional[str] = None,
) -> None:
    """Fire-and-forget: write one LLM call record to llm_usage. Never raises."""
    try:
        db = DatabaseManager.get_instance()
        db.record_llm_usage(
            call_type=call_type,
            model=model,
            prompt_tokens=usage.get("prompt_tokens", 0) or 0,
            completion_tokens=usage.get("completion_tokens", 0) or 0,
            total_tokens=usage.get("total_tokens", 0) or 0,
            stock_code=stock_code,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("[LLM usage] failed to persist usage record: %s", exc)


if __name__ == "__main__":
    # testingcode
    logging.basicConfig(level=logging.DEBUG)
    
    db = get_db()
    
    print("=== databasetesting ===")
    print(f"databaseinitializingsuccessful")
    
    # testingchecktodaydata
    has_data = db.has_today_data('600519')
    print(f"Maotaitodaywhetherhasdata: {has_data}")
    
    # testingsavingdata
    test_df = pd.DataFrame({
        'date': [date.today()],
        'open': [1800.0],
        'high': [1850.0],
        'low': [1780.0],
        'close': [1820.0],
        'volume': [10000000],
        'amount': [18200000000],
        'pct_chg': [1.5],
        'ma5': [1810.0],
        'ma10': [1800.0],
        'ma20': [1790.0],
        'volume_ratio': [1.2],
    })
    
    saved = db.save_daily_data(test_df, '600519', 'TestSource')
    print(f"savingtestingdata: {saved} items")
    
    # testinggetcontext
    context = db.get_analysis_context('600519')
    print(f"analyzingcontext: {context}")
