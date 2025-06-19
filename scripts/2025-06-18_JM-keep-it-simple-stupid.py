import os
import sqlite3
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Set
from pathlib import Path
import logging
from pydantic import BaseModel, Field, model_validator
from dataclasses import dataclass
import time
import tempfile
from dotenv import load_dotenv
import csv
import re
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
ERCOT_AUTH_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
ERCOT_API_BASE = "https://api.ercot.com/api/public-reports"

# Get credentials from environment
USERNAME = os.getenv("ERCOT_API_USERNAME")
PASSWORD = os.getenv("ERCOT_API_PASSWORD")
SUBSCRIPTION_KEY = os.getenv("ERCOT_API_SUBSCRIPTION_KEY")

if not all([USERNAME, PASSWORD, SUBSCRIPTION_KEY]):
    raise ValueError("Missing required environment variables")

logger.info(f"Loaded credentials for user: {USERNAME}")
logger.debug(f"Subscription key: {SUBSCRIPTION_KEY[:10]}...")
CHUNK_SIZE = 10000  # Process data in chunks
BATCH_SIZE = 1000   # SQLite batch insert size

# --- Pydantic BaseModel normalization mixin ---


class NormalizedBaseModel(BaseModel):
    @model_validator(mode="before")
    @classmethod
    def normalize_keys(cls, values):
        # ensure dict and normalize field keys to all-caps
        if isinstance(values, dict):
            values = normalize_dict_keys(values)
            # map raw 'SETTLEMENTPOINT' to 'SETTLEMENTPOINTNAME'
            if ('SETTLEMENTPOINT' in values and
                    'SETTLEMENTPOINTNAME' not in values):
                values['SETTLEMENTPOINTNAME'] = values.pop('SETTLEMENTPOINT')

            # map various BID ID field variations to BIDID
            bid_id_variations = [
                'ENERGYONLYBIDID', 'ENERGYONLY_BID_ID', 'BID_ID'
            ]
            for bid_variation in bid_id_variations:
                if bid_variation in values and 'BIDID' not in values:
                    values['BIDID'] = values.pop(bid_variation)

            # map various OFFER ID field variations to OFFERID
            offer_id_variations = [
                'ENERGYONLYOFFERID', 'ENERGYONLY_OFFER_ID', 'OFFER_ID'
            ]
            for offer_variation in offer_id_variations:
                if offer_variation in values and 'OFFERID' not in values:
                    values['OFFERID'] = values.pop(offer_variation)

            # Convert empty strings to None for numeric fields
            for key, value in list(values.items()):
                if isinstance(value, str) and value.strip() == '':
                    # Check if this is a numeric field that should be None
                    if any(pattern in key for pattern in ['MW', 'PRICE', 'AWARD']):
                        values[key] = None
        return values

# Pydantic Models for Data Validation


class DAMEnergyBidAward(NormalizedBaseModel):
    DELIVERYDATE: str = Field(alias="Delivery Date")
    HOURENDING: int = Field(alias="Hour Ending")
    SETTLEMENTPOINTNAME: str = Field(alias="Settlement Point")
    QSENAME: str = Field(alias="QSE Name")
    ENERGYONLYBIDAWARDINMW: Optional[float] = Field(
        alias="Energy Only Bid Award in MW")
    SETTLEMENTPOINTPRICE: Optional[float] = Field(
        alias="Settlement Point Price")
    BIDID: str = Field(alias="Bid ID")

    class Config:
        validate_by_name = True
        allow_population_by_field_name = True

    # Snake-case accessors
    @property
    def delivery_date(self):
        return self.DELIVERYDATE

    @property
    def hour_ending(self):
        return self.HOURENDING

    @property
    def settlement_point_name(self):
        return self.SETTLEMENTPOINTNAME

    @property
    def qse_name(self):
        return self.QSENAME

    @property
    def energy_only_bid_award_in_mw(self):
        return self.ENERGYONLYBIDAWARDINMW

    @property
    def settlement_point_price(self):
        return self.SETTLEMENTPOINTPRICE

    @property
    def bid_id(self):
        return self.BIDID


class DAMEnergyBid(NormalizedBaseModel):
    DELIVERYDATE: str = Field(alias="Delivery Date")
    HOURENDING: int = Field(alias="Hour Ending")
    SETTLEMENTPOINTNAME: str = Field(alias="Settlement Point")
    QSENAME: str = Field(alias="QSE Name")
    BIDID: Union[str, float] = Field(alias="Bid ID")
    MULTIHOURBLOCK: Optional[bool] = Field(
        alias="Multi-Hour Block Indicator", default=None)
    BLOCKCURVE: Optional[bool] = Field(
        alias="Block/Curve indicator", default=None)

    # Energy bid price/MW pairs (1-10)
    ENERGYONLYBIDMW1: Optional[float] = Field(
        alias="Energy Only Bid MW1", default=None)
    ENERGYONLYBIDPRICE1: Optional[float] = Field(
        alias="Energy Only Bid Price1", default=None)
    ENERGYONLYBIDMW2: Optional[float] = Field(
        alias="Energy Only Bid MW2", default=None)
    ENERGYONLYBIDPRICE2: Optional[float] = Field(
        alias="Energy Only Bid Price2", default=None)
    ENERGYONLYBIDMW3: Optional[float] = Field(
        alias="Energy Only Bid MW3", default=None)
    ENERGYONLYBIDPRICE3: Optional[float] = Field(
        alias="Energy Only Bid Price3", default=None)
    ENERGYONLYBIDMW4: Optional[float] = Field(
        alias="Energy Only Bid MW4", default=None)
    ENERGYONLYBIDPRICE4: Optional[float] = Field(
        alias="Energy Only Bid Price4", default=None)
    ENERGYONLYBIDMW5: Optional[float] = Field(
        alias="Energy Only Bid MW5", default=None)
    ENERGYONLYBIDPRICE5: Optional[float] = Field(
        alias="Energy Only Bid Price5", default=None)
    ENERGYONLYBIDMW6: Optional[float] = Field(
        alias="Energy Only Bid MW6", default=None)
    ENERGYONLYBIDPRICE6: Optional[float] = Field(
        alias="Energy Only Bid Price6", default=None)
    ENERGYONLYBIDMW7: Optional[float] = Field(
        alias="Energy Only Bid MW7", default=None)
    ENERGYONLYBIDPRICE7: Optional[float] = Field(
        alias="Energy Only Bid Price7", default=None)
    ENERGYONLYBIDMW8: Optional[float] = Field(
        alias="Energy Only Bid MW8", default=None)
    ENERGYONLYBIDPRICE8: Optional[float] = Field(
        alias="Energy Only Bid Price8", default=None)
    ENERGYONLYBIDMW9: Optional[float] = Field(
        alias="Energy Only Bid MW9", default=None)
    ENERGYONLYBIDPRICE9: Optional[float] = Field(
        alias="Energy Only Bid Price9", default=None)
    ENERGYONLYBIDMW10: Optional[float] = Field(
        alias="Energy Only Bid MW10", default=None)
    ENERGYONLYBIDPRICE10: Optional[float] = Field(
        alias="Energy Only Bid Price10", default=None)

    @staticmethod
    def parse_block_curve(v):
        if v is None or (hasattr(v, 'isna') and v.isna()):
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            v = v.strip().upper()
            if v in ("Y", "YES", "TRUE", "T", "1", "V"):
                return True
            if v in ("N", "NO", "FALSE", "F", "0"):
                return False
        return None

    @staticmethod
    def parse_multi_hour_block(v):
        if v is None or (hasattr(v, 'isna') and v.isna()):
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            v = v.strip().upper()
            if v in ("Y", "YES", "TRUE", "T", "1", "V"):
                return True
            if v in ("N", "NO", "FALSE", "F", "0"):
                return False
        return None

    @staticmethod
    def empty_str_to_none(v):
        if v == '' or (isinstance(v, str) and v.strip() == ''):
            return None
        return v

    class Config:
        validate_by_name = True
        allow_population_by_field_name = True

    # Snake-case accessors
    @property
    def delivery_date(self):
        return self.DELIVERYDATE

    @property
    def hour_ending(self):
        return self.HOURENDING

    @property
    def settlement_point_name(self):
        return self.SETTLEMENTPOINTNAME

    @property
    def qse_name(self):
        return self.QSENAME

    @property
    def bid_id(self):
        return self.BIDID

    @property
    def multi_hour_block(self):
        return self.MULTIHOURBLOCK

    @property
    def block_curve(self):
        return self.BLOCKCURVE

    # Energy-only bid MW and price accessors
    @property
    def energy_only_bid_mw1(self):
        return self.ENERGYONLYBIDMW1

    @property
    def energy_only_bid_price1(self):
        return self.ENERGYONLYBIDPRICE1

    @property
    def energy_only_bid_mw2(self):
        return self.ENERGYONLYBIDMW2

    @property
    def energy_only_bid_price2(self):
        return self.ENERGYONLYBIDPRICE2

    @property
    def energy_only_bid_mw3(self):
        return self.ENERGYONLYBIDMW3

    @property
    def energy_only_bid_price3(self):
        return self.ENERGYONLYBIDPRICE3

    @property
    def energy_only_bid_mw4(self):
        return self.ENERGYONLYBIDMW4

    @property
    def energy_only_bid_price4(self):
        return self.ENERGYONLYBIDPRICE4

    @property
    def energy_only_bid_mw5(self):
        return self.ENERGYONLYBIDMW5

    @property
    def energy_only_bid_price5(self):
        return self.ENERGYONLYBIDPRICE5

    @property
    def energy_only_bid_mw6(self):
        return self.ENERGYONLYBIDMW6

    @property
    def energy_only_bid_price6(self):
        return self.ENERGYONLYBIDPRICE6

    @property
    def energy_only_bid_mw7(self):
        return self.ENERGYONLYBIDMW7

    @property
    def energy_only_bid_price7(self):
        return self.ENERGYONLYBIDPRICE7

    @property
    def energy_only_bid_mw8(self):
        return self.ENERGYONLYBIDMW8

    @property
    def energy_only_bid_price8(self):
        return self.ENERGYONLYBIDPRICE8

    @property
    def energy_only_bid_mw9(self):
        return self.ENERGYONLYBIDMW9

    @property
    def energy_only_bid_price9(self):
        return self.ENERGYONLYBIDPRICE9

    @property
    def energy_only_bid_mw10(self):
        return self.ENERGYONLYBIDMW10

    @property
    def energy_only_bid_price10(self):
        return self.ENERGYONLYBIDPRICE10


class DAMEnergyOfferAward(NormalizedBaseModel):
    DELIVERYDATE: str = Field(alias="Delivery Date")
    HOURENDING: int = Field(alias="Hour Ending")
    SETTLEMENTPOINTNAME: str = Field(alias="Settlement Point")
    QSENAME: str = Field(alias="QSE Name")
    ENERGYONLYOFFERAWARDINMW: Optional[float] = Field(
        alias="Energy Only Offer Award in MW")
    SETTLEMENTPOINTPRICE: Optional[float] = Field(
        alias="Settlement Point Price")
    OFFERID: str = Field(alias="Offer ID")

    class Config:
        validate_by_name = True
        allow_population_by_field_name = True

    # Snake-case accessors
    @property
    def delivery_date(self):
        return self.DELIVERYDATE

    @property
    def hour_ending(self):
        return self.HOURENDING

    @property
    def settlement_point_name(self):
        return self.SETTLEMENTPOINTNAME

    @property
    def qse_name(self):
        return self.QSENAME

    @property
    def energy_only_offer_award_in_mw(self):
        return self.ENERGYONLYOFFERAWARDINMW

    @property
    def settlement_point_price(self):
        return self.SETTLEMENTPOINTPRICE

    @property
    def offer_id(self):
        return self.OFFERID


class DAMEnergyOffer(NormalizedBaseModel):
    DELIVERYDATE: str = Field(alias="Delivery Date")
    HOURENDING: int = Field(alias="Hour Ending")
    SETTLEMENTPOINTNAME: str = Field(alias="Settlement Point")
    QSENAME: str = Field(alias="QSE Name")
    OFFERID: Union[str, float] = Field(alias="Energy Only Offer ID")
    MULTIHOURBLOCK: Optional[bool] = Field(
        alias="Multi-Hour Block Indicator", default=None)
    BLOCKCURVE: Optional[bool] = Field(
        alias="Block/Curve indicator", default=None)

    # Energy offer price/MW pairs (1-10) - all fields Optional, with correct aliases
    ENERGYONLYOFFERMW1: Optional[float] = Field(
        alias="Energy Only Offer MW1", default=None)
    ENERGYONLYOFFERPRICE1: Optional[float] = Field(
        alias="Energy Only Offer Price1", default=None)
    ENERGYONLYOFFERMW2: Optional[float] = Field(
        alias="Energy Only Offer MW2", default=None)
    ENERGYONLYOFFERPRICE2: Optional[float] = Field(
        alias="Energy Only Offer Price2", default=None)
    ENERGYONLYOFFERMW3: Optional[float] = Field(
        alias="Energy Only Offer MW3", default=None)
    ENERGYONLYOFFERPRICE3: Optional[float] = Field(
        alias="Energy Only Offer Price3", default=None)
    ENERGYONLYOFFERMW4: Optional[float] = Field(
        alias="Energy Only Offer MW4", default=None)
    ENERGYONLYOFFERPRICE4: Optional[float] = Field(
        alias="Energy Only Offer Price4", default=None)
    ENERGYONLYOFFERMW5: Optional[float] = Field(
        alias="Energy Only Offer MW5", default=None)
    ENERGYONLYOFFERPRICE5: Optional[float] = Field(
        alias="Energy Only Offer Price5", default=None)
    ENERGYONLYOFFERMW6: Optional[float] = Field(
        alias="Energy Only Offer MW6", default=None)
    ENERGYONLYOFFERPRICE6: Optional[float] = Field(
        alias="Energy Only Offer Price6", default=None)
    ENERGYONLYOFFERMW7: Optional[float] = Field(
        alias="Energy Only Offer MW7", default=None)
    ENERGYONLYOFFERPRICE7: Optional[float] = Field(
        alias="Energy Only Offer Price7", default=None)
    ENERGYONLYOFFERMW8: Optional[float] = Field(
        alias="Energy Only Offer MW8", default=None)
    ENERGYONLYOFFERPRICE8: Optional[float] = Field(
        alias="Energy Only Offer Price8", default=None)
    ENERGYONLYOFFERMW9: Optional[float] = Field(
        alias="Energy Only Offer MW9", default=None)
    ENERGYONLYOFFERPRICE9: Optional[float] = Field(
        alias="Energy Only Offer Price9", default=None)
    ENERGYONLYOFFERMW10: Optional[float] = Field(
        alias="Energy Only Offer MW10", default=None)
    ENERGYONLYOFFERPRICE10: Optional[float] = Field(
        alias="Energy Only Offer Price10", default=None)

    @staticmethod
    def parse_block_curve(v):
        if v is None or (hasattr(v, 'isna') and v.isna()):
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            v = v.strip().upper()
            if v in ("Y", "YES", "TRUE", "T", "1", "V"):
                return True
            if v in ("N", "NO", "FALSE", "F", "0"):
                return False
        return None

    @staticmethod
    def parse_multi_hour_block(v):
        if v is None or (hasattr(v, 'isna') and v.isna()):
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            v = v.strip().upper()
            if v in ("Y", "YES", "TRUE", "T", "1", "V"):
                return True
            if v in ("N", "NO", "FALSE", "F", "0"):
                return False
        return None

    @staticmethod
    def empty_str_to_none(v):
        if v == '' or (isinstance(v, str) and v.strip() == ''):
            return None
        return v

    class Config:
        validate_by_name = True
        allow_population_by_field_name = True

    # Snake-case accessors
    @property
    def delivery_date(self):
        return self.DELIVERYDATE

    @property
    def hour_ending(self):
        return self.HOURENDING

    @property
    def settlement_point_name(self):
        return self.SETTLEMENTPOINTNAME

    @property
    def qse_name(self):
        return self.QSENAME

    @property
    def offer_id(self):
        return self.OFFERID

    @property
    def multi_hour_block(self):
        return self.MULTIHOURBLOCK

    @property
    def block_curve(self):
        return self.BLOCKCURVE

    # Energy-only offer MW and price accessors
    @property
    def energy_only_offer_mw1(self):
        return self.ENERGYONLYOFFERMW1

    @property
    def energy_only_offer_price1(self):
        return self.ENERGYONLYOFFERPRICE1

    @property
    def energy_only_offer_mw2(self):
        return self.ENERGYONLYOFFERMW2

    @property
    def energy_only_offer_price2(self):
        return self.ENERGYONLYOFFERPRICE2

    @property
    def energy_only_offer_mw3(self):
        return self.ENERGYONLYOFFERMW3

    @property
    def energy_only_offer_price3(self):
        return self.ENERGYONLYOFFERPRICE3

    @property
    def energy_only_offer_mw4(self):
        return self.ENERGYONLYOFFERMW4

    @property
    def energy_only_offer_price4(self):
        return self.ENERGYONLYOFFERPRICE4

    @property
    def energy_only_offer_mw5(self):
        return self.ENERGYONLYOFFERMW5

    @property
    def energy_only_offer_price5(self):
        return self.ENERGYONLYOFFERPRICE5

    @property
    def energy_only_offer_mw6(self):
        return self.ENERGYONLYOFFERMW6

    @property
    def energy_only_offer_price6(self):
        return self.ENERGYONLYOFFERPRICE6

    @property
    def energy_only_offer_mw7(self):
        return self.ENERGYONLYOFFERMW7

    @property
    def energy_only_offer_price7(self):
        return self.ENERGYONLYOFFERPRICE7

    @property
    def energy_only_offer_mw8(self):
        return self.ENERGYONLYOFFERMW8

    @property
    def energy_only_offer_price8(self):
        return self.ENERGYONLYOFFERPRICE8

    @property
    def energy_only_offer_mw9(self):
        return self.ENERGYONLYOFFERMW9

    @property
    def energy_only_offer_price9(self):
        return self.ENERGYONLYOFFERPRICE9

    @property
    def energy_only_offer_mw10(self):
        return self.ENERGYONLYOFFERMW10

    @property
    def energy_only_offer_price10(self):
        return self.ENERGYONLYOFFERPRICE10


class SPPData(NormalizedBaseModel):
    DELIVERYDATE: str = Field(alias="DeliveryDate")
    DELIVERYHOUR: int = Field(alias="DeliveryHour")
    DELIVERYINTERVAL: int = Field(alias="DeliveryInterval")
    SETTLEMENTPOINTNAME: str = Field(alias="SettlementPointName")
    SETTLEMENTPOINTTYPE: str = Field(alias="SettlementPointType")
    SETTLEMENTPOINTPRICE: Optional[float] = Field(alias="SettlementPointPrice")
    DSTFLAG: Optional[str] = Field(alias="DSTFlag")

    class Config:
        validate_by_name = True

    # Snake-case accessors
    @property
    def delivery_date(self):
        return self.DELIVERYDATE

    @property
    def delivery_hour(self):
        return self.DELIVERYHOUR

    @property
    def delivery_interval(self):
        return self.DELIVERYINTERVAL

    @property
    def settlement_point_name(self):
        return self.SETTLEMENTPOINTNAME

    @property
    def settlement_point_type(self):
        return self.SETTLEMENTPOINTTYPE

    @property
    def settlement_point_price(self):
        return self.SETTLEMENTPOINTPRICE

    @property
    def dst_flag(self):
        return self.DSTFLAG


class ERCOTTrackingQSE(NormalizedBaseModel):
    NAME: str = Field(alias="NAME")
    SHORTNAME: str = Field(alias="SHORT NAME")
    DUNSNUMBER: Optional[float] = Field(alias="DUNS NUMBER")
    MARKETPARTICIPANTTYPE: str = Field(alias="MARKET PARTICIPANT TYPE")
    REFDATE: str = Field(alias="REF_DATE")
    WEB01: Optional[str] = Field(alias="web_01")
    WEB02: Optional[str] = Field(alias="web_02")
    AWFLAG02: Optional[int] = Field(alias="AW_FLAG_02")
    ACTVPWRGRP: Optional[int] = Field(alias="ACTV_PWR_GRP")

    class Config:
        validate_by_name = True

    # Snake-case accessors
    @property
    def name(self):
        return self.NAME

    @property
    def short_name(self):
        return self.SHORTNAME

    @property
    def duns_number(self):
        return self.DUNSNUMBER

    @property
    def market_participant_type(self):
        return self.MARKETPARTICIPANTTYPE

    @property
    def ref_date(self):
        return self.REFDATE

    @property
    def web_01(self):
        return self.WEB01

    @property
    def web_02(self):
        return self.WEB02

    @property
    def aw_flag_02(self):
        return self.AWFLAG02

    @property
    def actv_pwr_grp(self):
        return self.ACTVPWRGRP


@dataclass
class AuthToken:
    access_token: str
    expires_at: datetime


class ERCOTDataPipeline:
    def __init__(self, db_path: str = "ercot_data.db", checkpoint_file: str = "pipeline_checkpoint.json", base_log_dir: str = "logs"):
        # Initialize tracking QSEs dictionary first
        self.tracked_qses = {}
        self.tracking_qse_short_names = set()

        # Setup structured logging first
        self.setup_logging(base_log_dir)

        self.db_path = db_path
        self.token: Optional[AuthToken] = None
        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_data = self.load_checkpoint()
        self.token_expires_at = None
        self.min_request_interval = 2.0  # 2 seconds between requests
        self.last_request_time = 0
        self.session = requests.Session()

        # Initialize session with fixed subscription key that never changes
        self.session.headers.update({
            "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY
        })

        self.setup_session_with_retries()

        # Track QSE -> Settlement Points mapping
        self.qse_settlement_points: Dict[str, Set[str]] = {}

        # Setup database
        self.setup_database()

        # Load tracking QSEs
        self.load_tracking_qses()

        self.ensure_dam_metadata_flags()
        self.ensure_spp_metadata_flag()

    def setup_logging(self, base_log_dir: str = "logs"):
        """Setup structured logging with timestamped directories and multiple log levels"""
        # Create timestamped directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = Path(base_log_dir) / f"pipeline_run_{timestamp}"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Remove existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )

        # Setup file handlers for different log levels
        log_files = {
            'debug': (logging.DEBUG, detailed_formatter),
            'info': (logging.INFO, simple_formatter),
            'warning': (logging.WARNING, simple_formatter),
            'error': (logging.ERROR, detailed_formatter),
            'all': (logging.DEBUG, detailed_formatter)  # Complete log
        }

        for log_name, (level, formatter) in log_files.items():
            file_handler = logging.FileHandler(
                self.log_dir / f"{log_name}.log")
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logging.root.addHandler(file_handler)

        # Console handler (INFO and above)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        logging.root.addHandler(console_handler)

        # Set root logger level
        logging.root.setLevel(logging.DEBUG)

        logger.info(f"Logging initialized - Log directory: {self.log_dir}")

    def log_csv_preview(self, file_path: str, file_type: str, max_rows: int = 5):
        """Log first few rows of CSV file for debugging"""
        try:
            logger.debug(f"=== {file_type} CSV Preview: {file_path} ===")
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if i >= max_rows:
                        break
                    logger.debug(f"{file_type} Row {i+1}: {row}")
            logger.debug(f"=== End {file_type} CSV Preview ===")
        except Exception as e:
            logger.warning(
                f"Could not preview {file_type} file {file_path}: {e}")

    # --- Existing methods remain unchanged ---
    def setup_session_with_retries(self):
        retry_strategy = Retry(
            total=3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def load_checkpoint(self) -> Dict:
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                logger.info(
                    f"Loaded checkpoint: {len(data.get('completed_dates', []))} dates already processed")
                return data
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
        return {
            'completed_dates': [],
            'failed_dates': [],
            'last_run_start': None,
            'last_run_end': None,
            'dam_archives_cache': {},
            'spp_documents_cache': {}
        }

    def save_checkpoint(self):
        try:
            self.checkpoint_data['last_updated'] = datetime.now().isoformat()
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2, default=str)
            logger.debug(
                f"Checkpoint saved: {len(self.checkpoint_data['completed_dates'])} completed dates")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def mark_date_completed(self, spp_date: datetime, dam_date: datetime):
        """Mark date as completed by setting all processing flags in metadata tables"""
        logger.info(
            f"Marking SPP {spp_date.date()} / DAM {dam_date.date()} as completed")

        # Mark all DAM processing as completed for this date
        self.mark_dam_processing_completed(dam_date)

        # Mark all SPP processing as completed for this date
        self.mark_spp_processing_completed(spp_date)

        # Keep checkpoint for backward compatibility (optional)
        date_entry = {
            'spp_date': spp_date.isoformat(),
            'dam_date': dam_date.isoformat(),
            'completed_at': datetime.now().isoformat()
        }
        self.checkpoint_data['completed_dates'].append(date_entry)
        self.save_checkpoint()

    def mark_dam_processing_completed(self, dam_date: datetime):
        """Mark all DAM processing flags as completed for a specific date"""
        dam_date_str = dam_date.strftime("%Y-%m-%d")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Set all DAM processing flags to 1 for archives on this date
        cursor.execute("""
            UPDATE DAMARCHIVESMETADATA
            SET BIDSEXTRACTED = 1,
                BIDAWARDSEXTRACTED = 1,
                OFFERSEXTRACTED = 1,
                OFFERAWARDSEXTRACTED = 1
            WHERE DATEEXTRACTED = ?
        """, (dam_date_str,))

        rows_updated = cursor.rowcount
        conn.commit()
        conn.close()

        logger.debug(
            f"Marked {rows_updated} DAM archives as completed for {dam_date_str}")

    def mark_spp_processing_completed(self, spp_date: datetime):
        """Mark all SPP processing flags as completed for a specific date"""
        spp_date_str = spp_date.strftime("%Y-%m-%d")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Set SPP extracted flag to 1 for documents on this date
        cursor.execute("""
            UPDATE SPPARCHIVESMETADATA
            SET EXTRACTED = 1
            WHERE DATEEXTRACTED = ?
        """, (spp_date_str,))

        rows_updated = cursor.rowcount
        conn.commit()
        conn.close()

        logger.debug(
            f"Marked {rows_updated} SPP documents as completed for {spp_date_str}")

    # def mark_date_failed(self, spp_date: datetime, dam_date: datetime, error: str):
    #     """Mark date as failed in checkpoint (metadata tables remain unchanged for retry)"""
    #     logger.warning(
    #         f"Marking SPP {spp_date.date()} / DAM {dam_date.date()} as failed: {error}")

    #     # Only update checkpoint - leave metadata flags unchanged so we can retry
    #     date_entry = {
    #         'spp_date': spp_date.isoformat(),
    #         'dam_date': dam_date.isoformat(),
    #         'error': str(error),
    #         'failed_at': datetime.now().isoformat()
    #     }
    #     self.checkpoint_data['failed_dates'].append(date_entry)
    #     self.save_checkpoint()

    # def is_date_completed(self, spp_date: datetime) -> bool:
    #     """Check if both DAM and SPP data processing is completed for a date pair using metadata tables"""
    #     dam_date = spp_date + timedelta(days=60)

    #     # Check if DAM processing is completed
    #     dam_completed = self.is_dam_processing_completed(dam_date)

    #     # Check if SPP processing is completed
    #     spp_completed = self.is_spp_processing_completed(spp_date)

    #     logger.debug(
    #         f"Date completion check for SPP {spp_date.date()}: DAM={dam_completed}, SPP={spp_completed}")
    #     return dam_completed and spp_completed

    # def is_dam_processing_completed(self, dam_date: datetime) -> bool:
    #     """Check if DAM processing is completed using metadata table flags"""
    #     dam_date_str = dam_date.strftime("%Y-%m-%d")

    #     conn = sqlite3.connect(self.db_path)
    #     cursor = conn.cursor()

    #     # Check if all DAM processing flags are set for this date
    #     cursor.execute("""
    #         SELECT COUNT(*) FROM DAMARCHIVESMETADATA
    #         WHERE DATEEXTRACTED = ?
    #         AND BIDSEXTRACTED = 1
    #         AND BIDAWARDSEXTRACTED = 1
    #         AND OFFERSEXTRACTED = 1
    #         AND OFFERAWARDSEXTRACTED = 1
    #     """, (dam_date_str,))

    #     completed_count = cursor.fetchone()[0]

    #     # Also check if there are any DAM archives for this date
    #     cursor.execute("""
    #         SELECT COUNT(*) FROM DAMARCHIVESMETADATA
    #         WHERE DATEEXTRACTED = ?
    #     """, (dam_date_str,))

    #     total_count = cursor.fetchone()[0]
    #     conn.close()

    #     if total_count == 0:
    #         logger.debug(f"No DAM archives found for {dam_date_str}")
    #         return False

    #     is_completed = completed_count > 0
    #     logger.debug(
    #         f"DAM processing for {dam_date_str}: {completed_count}/{total_count} archives fully processed")
    #     return is_completed

    # def is_spp_processing_completed(self, spp_date: datetime) -> bool:
    #     """Check if SPP processing is completed using metadata table flags"""
    #     spp_date_str = spp_date.strftime("%Y-%m-%d")

    #     conn = sqlite3.connect(self.db_path)
    #     cursor = conn.cursor()

    #     # Check if all SPP documents are extracted for this date
    #     cursor.execute("""
    #         SELECT COUNT(*) FROM SPPARCHIVESMETADATA
    #         WHERE DATEEXTRACTED = ? AND EXTRACTED = 1
    #     """, (spp_date_str,))

    #     completed_count = cursor.fetchone()[0]

    #     # Also check if there are any SPP documents for this date
    #     cursor.execute("""
    #         SELECT COUNT(*) FROM SPPARCHIVESMETADATA
    #         WHERE DATEEXTRACTED = ?
    #     """, (spp_date_str,))

    #     total_count = cursor.fetchone()[0]
    #     conn.close()

    #     if total_count == 0:
    #         logger.debug(f"No SPP documents found for {spp_date_str}")
    #         return False

    #     is_completed = completed_count > 0
    #     logger.debug(
    #         f"SPP processing for {spp_date_str}: {completed_count}/{total_count} documents processed")
    #     return is_completed

    def wait_for_rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def is_token_expired(self) -> bool:
        if not self.token_expires_at:
            return True
        return datetime.now() >= (self.token_expires_at - timedelta(minutes=5))

    def make_request_with_retry(self, url: str, params: Dict = None, max_retries: int = 5) -> requests.Response:
        for attempt in range(max_retries):
            if self.is_token_expired():
                logger.info("Token expired or expiring soon, refreshing...")
                if not self.authenticate():
                    raise Exception("Failed to refresh authentication token")

            self.wait_for_rate_limit()

            try:
                logger.debug(
                    f"Making request to {url} (attempt {attempt + 1}/{max_retries})")

                # Get fresh headers with Bearer token for each request
                headers = self.get_bearer_headers()
                auth_header = headers.get('Authorization', 'None')[:30]
                sub_key = headers.get('Ocp-Apim-Subscription-Key', 'None')
                logger.debug(
                    f"Headers: Authorization={auth_header}..., Subscription-Key={sub_key}")

                # Use headers parameter instead of session headers
                response = requests.get(url, params=params, headers=headers)
                if response.status_code == 200:
                    return response
                elif response.status_code == 401:
                    logger.warning("Received 401, refreshing token...")
                    # Force token refresh on 401
                    self.token_expires_at = None
                    if not self.authenticate():
                        raise Exception(
                            "Failed to refresh authentication token after 401")
                    continue
                elif response.status_code == 429:
                    backoff_time = min(300, (2 ** attempt) * 2)
                    logger.warning(
                        f"Rate limited (429). Backing off for {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    continue
                else:
                    logger.error(
                        f"HTTP {response.status_code}: {response.text[:200]}")
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                backoff_time = min(60, (2 ** attempt) * 2)
                logger.warning(
                    f"Request failed: {e}. Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
        raise Exception(
            f"Failed to complete request after {max_retries} attempts")

    def setup_database(self):
        """Create database tables if they don't exist, using normalized all-caps, no-special-chars schema. Migrate if needed."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # List of expected tables and columns (normalized)
        expected_schemas = {
            'DAMENERGYBIDAWARDS': [
                'DELIVERYDATE', 'HOURENDING', 'SETTLEMENTPOINTNAME', 'QSENAME',
                'ENERGYONLYBIDAWARDINMW', 'SETTLEMENTPOINTPRICE', 'BIDID', 'INSERTEDAT'
            ],
            'DAMENERGYBIDS': [
                'DELIVERYDATE', 'HOURENDING', 'SETTLEMENTPOINTNAME', 'QSENAME', 'BIDID',
                'MULTIHOURBLOCK', 'BLOCKCURVE', 'ENERGYONLYBIDMW', 'ENERGYONLYBIDPRICE', 'BIDSEGMENT', 'INSERTEDAT'
            ],
            'DAMENERGYOFFERAWARDS': [
                'DELIVERYDATE', 'HOURENDING', 'SETTLEMENTPOINTNAME', 'QSENAME',
                'ENERGYONLYOFFERAWARDINMW', 'SETTLEMENTPOINTPRICE', 'OFFERID', 'INSERTEDAT'
            ],
            'DAMENERGYOFFERS': [
                'DELIVERYDATE', 'HOURENDING', 'SETTLEMENTPOINTNAME', 'QSENAME', 'OFFERID',
                'MULTIHOURBLOCK', 'BLOCKCURVE', 'ENERGYONLYOFFERMW', 'ENERGYONLYOFFERPRICE', 'OFFERSEGMENT', 'INSERTEDAT'
            ],
            'SETTLEMENTPOINTPRICES': [
                'DELIVERYDATE', 'DELIVERYHOUR', 'SETTLEMENTPOINT', 'SETTLEMENTPOINTTYPE',
                'AVGSETTLEMENTPOINTPRICE', 'DSTFLAG', 'INSERTEDAT'
            ],
            'FINAL': [
                'DELIVERYDATE', 'HOURENDING', 'SETTLEMENTPOINTNAME', 'QSENAME',
                'SETTLEMENTPOINTPRICE', 'AVGSETTLEMENTPOINTPRICE', 'BLOCKCURVE', 'SOURCETYPE',
                'ENERGYONLYBIDAWARDINMW', 'BIDID', 'ENERGYONLYOFFERAWARDMW', 'OFFERID', 'INSERTEDAT'
            ]
        }
        # Create or migrate each table
        for table, columns in expected_schemas.items():
            cursor.execute(f"PRAGMA table_info({table})")
            current_cols = [row[1] for row in cursor.fetchall()]
            norm_current_cols = [
                re.sub(r'[^A-Za-z0-9]', '', c).upper() for c in current_cols]
            norm_expected_cols = [
                re.sub(r'[^A-Za-z0-9]', '', c).upper() for c in columns]
            if norm_current_cols and norm_current_cols != norm_expected_cols:
                logger.info(f"Migrating table {table} to normalized schema...")
                # Build CREATE TABLE statement for new table
                col_defs = ', '.join([f'{col} TEXT' for col in columns])
                cursor.execute(f"CREATE TABLE {table}_NEW ({col_defs})")
                # Map old columns to new columns by normalized name
                col_map = {nc: c for nc, c in zip(
                    norm_current_cols, current_cols)}
                insert_cols = []
                select_cols = []
                for col in columns:
                    norm_col = re.sub(r'[^A-Za-z0-9]', '', col).upper()
                    if norm_col in col_map:
                        insert_cols.append(col)
                        select_cols.append(col_map[norm_col])
                    else:
                        insert_cols.append(col)
                        select_cols.append('NULL')
                cursor.execute(
                    f"INSERT INTO {table}_NEW ({', '.join(insert_cols)}) SELECT {', '.join(select_cols)} FROM {table}")
                cursor.execute(f"DROP TABLE {table}")
                cursor.execute(f"ALTER TABLE {table}_NEW RENAME TO {table}")
                logger.info(f"Table {table} migrated to normalized schema.")
        # --- Create tables if not exist ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DAMENERGYBIDAWARDS (
                DELIVERYDATE TEXT,
                HOURENDING INTEGER,
                SETTLEMENTPOINTNAME TEXT,
                QSENAME TEXT,
                ENERGYONLYBIDAWARDINMW REAL,
                SETTLEMENTPOINTPRICE REAL,
                BIDID TEXT,
                INSERTEDAT TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME, BIDID)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DAMENERGYBIDS (
                DELIVERYDATE TEXT,
                HOURENDING INTEGER,
                SETTLEMENTPOINTNAME TEXT,
                QSENAME TEXT,
                BIDID TEXT,
                MULTIHOURBLOCK BOOLEAN,
                BLOCKCURVE BOOLEAN,
                ENERGYONLYBIDMW REAL,
                ENERGYONLYBIDPRICE REAL,
                BIDSEGMENT INTEGER,
                INSERTEDAT TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME, BIDID, BIDSEGMENT)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DAMENERGYOFFERAWARDS (
                DELIVERYDATE TEXT,
                HOURENDING INTEGER,
                SETTLEMENTPOINTNAME TEXT,
                QSENAME TEXT,
                ENERGYONLYOFFERAWARDINMW REAL,
                SETTLEMENTPOINTPRICE REAL,
                OFFERID TEXT,
                INSERTEDAT TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME, OFFERID)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DAMENERGYOFFERS (
                DELIVERYDATE TEXT,
                HOURENDING INTEGER,
                SETTLEMENTPOINTNAME TEXT,
                QSENAME TEXT,
                OFFERID TEXT,
                MULTIHOURBLOCK BOOLEAN,
                BLOCKCURVE BOOLEAN,
                ENERGYONLYOFFERMW REAL,
                ENERGYONLYOFFERPRICE REAL,
                OFFERSEGMENT INTEGER,
                INSERTEDAT TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME, OFFERID, OFFERSEGMENT)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS SETTLEMENTPOINTPRICES (
                DELIVERYDATE TEXT,
                DELIVERYHOUR INTEGER,
                SETTLEMENTPOINT TEXT,
                SETTLEMENTPOINTTYPE TEXT,
                AVGSETTLEMENTPOINTPRICE REAL,
                DSTFLAG BOOLEAN,
                INSERTEDAT TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (DELIVERYDATE, DELIVERYHOUR, SETTLEMENTPOINT)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS FINAL (
                DELIVERYDATE TEXT,
                HOURENDING INTEGER,
                SETTLEMENTPOINTNAME TEXT,
                QSENAME TEXT,
                SETTLEMENTPOINTPRICE REAL,
                AVGSETTLEMENTPOINTPRICE REAL,
                BLOCKCURVE TEXT,
                SOURCETYPE TEXT,
                ENERGYONLYBIDAWARDINMW REAL,
                BIDID TEXT,
                ENERGYONLYOFFERAWARDMW REAL,
                OFFERID TEXT,
                INSERTEDAT TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME, SOURCETYPE)
            )
        ''')

        # Create DAM archives metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DAMARCHIVESMETADATA (
                DOCID INTEGER PRIMARY KEY,
                FRIENDLYNAME TEXT NOT NULL,
                POSTDATETIME TEXT NOT NULL,
                DOWNLOADURL TEXT NOT NULL,
                CACHEDATE TEXT NOT NULL,
                DATEEXTRACTED TEXT
            )
        """)

        # Create indexes for DAMARCHIVESMETADATA
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS IDXDAMPOSTDATETIME ON DAMARCHIVESMETADATA(POSTDATETIME)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS IDXDAMDATEEXTRACTED ON DAMARCHIVESMETADATA(DATEEXTRACTED)")

        # Create SPP archives metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SPPARCHIVESMETADATA (
                DOCID INTEGER PRIMARY KEY,
                FRIENDLYNAME TEXT NOT NULL,
                POSTDATETIME TEXT NOT NULL,
                DOWNLOADURL TEXT NOT NULL,
                CACHEDATE TEXT NOT NULL,
                DATEEXTRACTED TEXT
            )
        """)

        # Create indexes for spp_archives_metadata
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS IDXSPPDATEEXTRACTED ON SPPARCHIVESMETADATA(DATEEXTRACTED)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS IDXSPPPOSTDATETIME ON SPPARCHIVESMETADATA(POSTDATETIME)")

        # Create metadata cache status table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS METADATACACHESTATUS (
                CACHETYPE TEXT PRIMARY KEY,
                LASTUPDATED TEXT NOT NULL,
                TOTALRECORDS INTEGER NOT NULL,
                ISCOMPLETE BOOLEAN NOT NULL DEFAULT 0
            )
        """)

        # Create indexes for metadata cache status
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS IDXCACHELASTUPDATED ON METADATACACHESTATUS(LASTUPDATED)")

        # Create indexes for performance
        tables_indexes = [
            ("DAMENERGYBIDAWARDS", "IDXBIDAWARDSDATEHOUR",
             "(DELIVERYDATE, HOURENDING)"),
            ("DAMENERGYBIDAWARDS", "IDXBIDAWARDQSE", "(QSENAME)"),
            ("DAMENERGYBIDAWARDS", "IDXBIDAWARDSSETTLEMENT", "(SETTLEMENTPOINTNAME)"),
            ("DAMENERGYBIDS", "IDXBIDS_DATE_HOUR", "(DELIVERYDATE, HOURENDING)"),
            ("DAMENERGYBIDAWARDS", "IDXBIDSQSE", "(QSENAME)"),
            ("DAMENERGYOFFERAWARDS", "IDXOFFERAWARDSDATEHOUR",
             "(DELIVERYDATE, HOURENDING)"),
            ("DAMENERGYOFFERAWARDS", "IDXOFFERAWARDSQSE", "(QSENAME)"),
            ("DAMENERGYOFFERS", "IDXOFFERSDATEHOUR",
             "(DELIVERYDATE, HOURENDING)"),
            ("DAMENERGYOFFERS", "IDXOFFERSQSE", "(QSENAME)"),
            ("SETTLEMENTPOINTPRICES", "IDXSPPDATEHOUR",
             "(DELIVERYDATE, DELIVERYHOUR)"),  # <-- FIXED: use DELIVERYHOUR, not HOURENDING
            ("SETTLEMENTPOINTPRICES", "IDXSPPSETTLEMENT",
             "(SETTLEMENTPOINT)"),  # FIXED: use SETTLEMENTPOINT
            ("FINAL", "IDXFINALDATEHOUR", "(DELIVERYDATE, HOURENDING)"),
            ("FINAL", "IDXFINALQSE", "(QSENAME)"),
            ("FINAL", "IDXFINALSETTLEMENTPOINT", "(SETTLEMENTPOINTNAME)")
        ]

        for table, index_name, columns in tables_indexes:
            cursor.execute(
                f'CREATE INDEX IF NOT EXISTS {index_name} ON {table} {columns}')

        conn.commit()
        conn.close()
        logger.info("Database setup completed with normalized schemas.")

    def migrate_table_to_new_schema(conn, table_name, expected_columns):
        """
        If the table exists and its columns do not match the expected (normalized) schema, migrate it:
        - Create a new table with the correct schema (table_name + '_new')
        - Copy data from the old table, mapping columns by normalized name
        - Drop the old table and rename the new one
        """
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        current_cols = [row[1] for row in cursor.fetchall()]
        norm_current_cols = [re.sub(r'[^A-Za-z0-9]', '', c).upper()
                             for c in current_cols]
        norm_expected_cols = [
            re.sub(r'[^A-Za-z0-9]', '', c).upper() for c in expected_columns]
        if norm_current_cols == norm_expected_cols:
            logger.info(f"Table {table_name} already matches new schema.")
            return
        logger.info(f"Migrating table {table_name} to new schema...")
        # Build CREATE TABLE statement for new table
        col_defs = ', '.join([f'{col} TEXT' for col in expected_columns])
        cursor.execute(f"CREATE TABLE {table_name}_new ({col_defs})")
        # Map old columns to new columns by normalized name
        col_map = {nc: c for nc, c in zip(norm_current_cols, current_cols)}
        insert_cols = []
        select_cols = []
        for col in expected_columns:
            norm_col = re.sub(r'[^A-Za-z0-9]', '', col).upper()
            if norm_col in col_map:
                insert_cols.append(col)
                select_cols.append(col_map[norm_col])
            else:
                insert_cols.append(col)
                select_cols.append('NULL')
        cursor.execute(
            f"INSERT INTO {table_name}_new ({', '.join(insert_cols)}) SELECT {', '.join(select_cols)} FROM {table_name}")
        cursor.execute(f"DROP TABLE {table_name}")
        cursor.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
        conn.commit()
        logger.info(f"Table {table_name} migrated to new schema.")

    def load_tracking_qses(self):
        """Load QSEs to track from the CSV file"""
        try:
            csv_path = Path("_data/ERCOT_tracking_list.csv")
            if not csv_path.exists():
                logger.error(f"Tracking QSE CSV file not found: {csv_path}")
                return

            logger.debug(f"Loading tracking QSEs from {csv_path}")

            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                qse_count = 0

                for row in reader:
                    try:
                        # Normalize keys before passing to model
                        row = normalize_dict_keys(row)
                        qse = ERCOTTrackingQSE(**row)
                        # Use normalized field name
                        self.tracked_qses[qse.SHORTNAME] = qse
                        self.tracking_qse_short_names.add(qse.SHORTNAME)
                        qse_count += 1
                        if qse_count <= 5:
                            logger.debug(
                                f"Loaded QSE: {qse.SHORTNAME} - {qse.NAME}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse QSE row: {row}. Error: {e}")
                        continue
            logger.info(f"Loaded {qse_count} tracked QSEs")
            logger.debug(
                f"QSE short names: {sorted(list(self.tracking_qse_short_names))[:10]}...")
        except Exception as e:
            logger.error(f"Failed to load tracking QSEs: {e}")
            self.tracked_qses = {}
            self.tracking_qse_short_names = set()

    def authenticate(self) -> bool:
        logger.debug("Authenticating with ERCOT API...")
        auth_url = (f"{ERCOT_AUTH_URL}?username={USERNAME}&password={PASSWORD}"
                    f"&grant_type=password"
                    f"&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access"
                    f"&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70"
                    f"&response_type=id_token")
        try:
            response = requests.post(auth_url, headers={}, data={})
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response text: {response.text[:500]}...")
            response.raise_for_status()
            token_data = response.json()
            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)
            try:
                expires_in = int(expires_in)
            except (ValueError, TypeError):
                expires_in = 3600
            if access_token:
                self.token = AuthToken(
                    access_token=access_token,
                    expires_at=datetime.now() + timedelta(seconds=expires_in)
                )
                self.token_expires_at = self.token.expires_at
                logger.info("Authentication successful")
                logger.debug(f"Token expires at: {self.token_expires_at}")
                logger.debug(f"Updated Bearer token: {access_token[:20]}...")
                return True
            else:
                logger.error(f"No access_token in response: {token_data}")
                self.token = None
                return False
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            if 'response' in locals():
                logger.debug(f"Response status: {response.status_code}")
                logger.debug(f"Response text: {response.text[:500]}...")
            self.token = None
            return False

    def get_dam_archive_ids(self) -> List[Dict]:
        """Get available DAM archive document IDs"""
        url = f"{ERCOT_API_BASE}/archive/NP3-966-er"
        response = self.session.get(url)
        response.raise_for_status()

        data = response.json()
        archives = data.get('archives', [])
        logger.info(f"Found {len(archives)} DAM archives")
        return archives

    # --- Utility: Normalize all dicts in a list ---
    def normalize_dicts_in_list(self, data):
        if isinstance(data, list):
            return [normalize_dict_keys(d) if isinstance(d, dict) else d for d in data]
        return data

    # --- Example: Normalize as soon as we get data from any source ---
    def get_all_dam_archives(self) -> List[Dict]:
        """Get all DAM archives with database caching"""

        # Check if we have fresh cached data
        if self.is_metadata_cache_fresh('dam_archives', max_age_hours=24):
            logger.info("Using cached DAM archives (less than 24 hours old)")
            return self.load_metadata_from_cache('dam_archives')

        logger.info("Fetching fresh DAM archives from API...")
        all_archives = []
        page = 1
        page_size = 1000

        try:
            while True:
                logger.debug(
                    f"Fetching DAM archives page {page} (size: {page_size})")
                url = f"{ERCOT_API_BASE}/archive/NP3-966-er"
                params = {
                    'page': page,
                    'size': page_size
                }

                response = self.make_request_with_retry(url, params)
                data = response.json()
                logger.debug(f"Response data: {data.keys()}")

                archives = data.get('archives', [])
                logger.debug(
                    f"Retrieved {len(archives)} archives from page {page}")

                if not archives:
                    logger.debug(
                        f"No more archives found on page {page}, stopping pagination")
                    break

                all_archives.extend(archives)

                # Check if we got fewer results than page size (last page)
                if len(archives) < page_size:
                    logger.debug(
                        f"Last page reached (got {len(archives)} < {page_size})")
                    break

                page += 1

            logger.info(
                f"Retrieved total of {len(all_archives)} DAM archives across {page-1} pages")

            # Save to cache
            self.save_metadata_to_cache('dam_archives', all_archives)

            return all_archives

        except Exception as e:
            logger.error(f"Failed to fetch DAM archives: {e}")
            # Try to return cached data even if it's old
            logger.info("Attempting to use stale cached data...")
            return self.load_metadata_from_cache('dam_archives')

    def get_all_spp_documents(self) -> List[Dict]:
        """Get all SPP documents with database caching"""

        # Check if we have fresh cached data
        if self.is_metadata_cache_fresh('spp_documents', max_age_hours=24):
            logger.info("Using cached SPP documents (less than 24 hours old)")
            return self.load_metadata_from_cache('spp_documents')

        logger.info("Fetching fresh SPP documents from API...")
        all_docs = []
        page = 1
        page_size = 1000

        try:
            while True:
                logger.debug(
                    f"Fetching SPP documents page {page} (size: {page_size})")
                url = f"{ERCOT_API_BASE}/archive/np6-905-cd"
                params = {
                    'page': page,
                    'size': page_size
                }

                logger.debug(f"Request URL: {url} with params: {params}")
                response = self.make_request_with_retry(url, params)
                data = response.json()
                logger.debug(f"Response data: {data.keys()}")
                docs = data.get('archives', [])
                logger.debug(
                    f"Retrieved {len(docs)} SPP documents from page {page}")

                if not docs:
                    logger.debug(
                        f"No more SPP documents found on page {page}, stopping pagination")
                    break

                all_docs.extend(docs)

                # Check if we got fewer results than page size (last page)
                if len(docs) < page_size:
                    logger.debug(
                        f"Last page reached (got {len(docs)} < {page_size})")
                    break

                page += 1

            logger.info(
                f"Retrieved total of {len(all_docs)} SPP documents across {page-1} pages")

            # Save to cache
            self.save_metadata_to_cache('spp_documents', all_docs)

            return all_docs

        except Exception as e:
            logger.error(f"Failed to fetch SPP documents: {e}")
            # Try to return cached data even if it's old
            logger.info("Attempting to use stale cached data...")
            return self.load_metadata_from_cache('spp_documents')

        logger.info(
            f"Retrieved total of {len(all_docs)} SPP documents across {page-1} pages")
        return all_docs

    def download_dam_archive(self, doc_id: str, temp_dir: str) -> str:
        """Download DAM archive file with robust retry and headers"""
        url = f"{ERCOT_API_BASE}/archive/NP3-966-er?download={doc_id}"
        # build headers including subscription key, bearer token, and Incapsula cookies
        # Ensure we have a valid token and subscription key
        headers = self.get_bearer_headers()
        # Add Incapsula cookies if required

        attempts = 0
        while True:
            # ensure at least 2s between requests to avoid rate limiting
            time.sleep(2)
            response = self.session.get(url, headers=headers, stream=True)
            # handle unauthorized: refresh token
            if response.status_code == 401:
                if self.authenticate():
                    headers.update(self.get_bearer_headers())
                    continue
                response.raise_for_status()
            # handle rate limit
            if response.status_code == 429:
                wait = 2 if attempts == 0 else 60
                time.sleep(wait)
                attempts += 1
                continue
            response.raise_for_status()
            break

        archive_path = os.path.join(temp_dir, f"dam_archive_{doc_id}.zip")
        with open(archive_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded DAM archive: {archive_path}")
        return archive_path

    def download_archive(self, doc_id: int, temp_dir: str, archive_type: str = "DAM") -> str:
        """Download an archive using the download API with robust headers and retry"""
        logger.debug(
            f"Downloading {archive_type} archive with docId: {doc_id}")

        # --- METADATA CACHING: Use metadata cache for download URL if possible ---
        meta_lookup = None
        try:
            if archive_type == "DAM":
                meta_lookup = self.get_dam_doc_id_and_url(
                    datetime.strptime(str(doc_id), "%Y%m%d"))
            elif archive_type == "SPP":
                meta_lookup = self.get_spp_doc_id_and_url(
                    datetime.strptime(str(doc_id), "%Y%m%d"))
        except ValueError:
            # doc_id is not in YYYYMMDD format, skip metadata lookup
            logger.debug(
                f"doc_id {doc_id} is not in YYYYMMDD format, skipping metadata cache lookup")
            meta_lookup = None

        if meta_lookup:
            download_url = meta_lookup['download_url']
            logger.debug(f"(Metadata) Download URL: {download_url}")
        else:
            # Determine the correct endpoint based on archive type
            if archive_type == "DAM":
                download_url = f"{ERCOT_API_BASE}/archive/NP3-966-er?download={doc_id}"
            elif archive_type == "SPP":
                download_url = f"{ERCOT_API_BASE}/bundle/np6-905-cd?download={doc_id}"
            else:
                raise ValueError(f"Unknown archive type: {archive_type}")

            logger.debug(f"Download URL: {download_url}")

        # build headers
        headers = self.get_bearer_headers()
        headers['Ocp-Apim-Subscription-Key'] = SUBSCRIPTION_KEY
        headers['Cookie'] = '; '.join(
            f"{c.name}={c.value}" for c in self.session.cookies)
        attempts = 0
        while True:
            response = requests.get(download_url, stream=True, headers=headers)
            if response.status_code == 401:
                if self.authenticate():
                    headers.update(self.get_bearer_headers())
                    headers['Ocp-Apim-Subscription-Key'] = SUBSCRIPTION_KEY
                    headers['Cookie'] = '; '.join(
                        f"{c.name}={c.value}" for c in self.session.cookies)
                    continue
                response.raise_for_status()
            if response.status_code == 429:
                wait = 2 if attempts == 0 else 60
                time.sleep(wait)
                attempts += 1
                continue
            response.raise_for_status()
            break

        spp_path = os.path.join(temp_dir, f"spp_data_{doc_id}.csv")
        with open(spp_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size = os.path.getsize(spp_path)
        logger.info("Downloaded SPP data: %s (%d bytes)", spp_path, file_size)
        # Check if file is a zip archive
        is_zip = False
        try:
            with zipfile.ZipFile(spp_path, 'r') as zip_ref:
                is_zip = True
                file_list = zip_ref.namelist()
                logger.info(
                    f"SPP archive {spp_path} contains files: {file_list}")
                # Extract first CSV file found
                for name in file_list:
                    if name.lower().endswith('.csv'):
                        extract_path = os.path.join(temp_dir, name)
                        with zip_ref.open(name) as src, open(extract_path, 'wb') as tgt:
                            tgt.write(src.read())
                        logger.info(f"Extracted SPP CSV: {extract_path}")
                        spp_path = extract_path
                        break
        except zipfile.BadZipFile:
            is_zip = False
        # Log first 5 lines of the downloaded or extracted file
        try:
            with open(spp_path, 'r', encoding='utf-8') as f:
                for i in range(5):
                    logger.info("SPP file preview line %d: %s",
                                i+1, f.readline().strip())
        except Exception as e:
            logger.warning("Could not preview SPP file: %s", e)
        return spp_path

    def download_spp_data(self, doc_id: str, temp_dir: str) -> str:
        """Download SPP data file - wrapper around download_archive for SPP type"""
        logger.debug(f"Downloading SPP data with docId: {doc_id}")

        # Use the existing download_archive method with SPP type
        return self.download_archive(int(doc_id), temp_dir, archive_type="SPP")

    def process_dam_bid_awards(self, file_path: str):
        """Process DAM Energy Bid Awards into separate table with validation and deduplication"""
        logger.info(f"Processing DAM Bid Awards: {file_path}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        processed_count = 0
        batch_data = []

        try:
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
                chunk.columns = normalize_headers(chunk.columns)
                for _, row in chunk.iterrows():
                    row_dict = normalize_dict_keys(row.to_dict())
                    try:
                        bid_award = DAMEnergyBidAward(**row_dict)
                        batch_data.append((
                            bid_award.delivery_date,
                            bid_award.hour_ending,
                            bid_award.settlement_point_name,
                            bid_award.qse_name,
                            bid_award.energy_only_bid_award_in_mw,
                            bid_award.settlement_point_price,
                            bid_award.bid_id
                        ))
                        processed_count += 1
                    except Exception as e:
                        if processed_count < 10:  # Only log first 10 errors
                            logger.warning(
                                f"Skipping invalid bid award row: {e}")
                        continue

                    # Batch insert when we hit the batch size
                    if len(batch_data) >= BATCH_SIZE:
                        cursor.executemany('''
                            INSERT OR REPLACE INTO DAMENERGYBIDAWARDS (
                                DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                                ENERGYONLYBIDAWARDINMW, SETTLEMENTPOINTPRICE, BIDID
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', batch_data)
                        conn.commit()
                        batch_data = []

            # Insert any remaining data
            if batch_data:
                cursor.executemany('''
                    INSERT OR REPLACE INTO DAMENERGYBIDAWARDS (
                        DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                        ENERGYONLYBIDAWARDINMW, SETTLEMENTPOINTPRICE, BIDID
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', batch_data)
                conn.commit()
        finally:
            conn.close()
        logger.info(f"Processed {processed_count} DAM bid awards")

    def process_dam_bids(self, file_path: str):
        """Process DAM Energy Bids with price/MW pair extraction into separate table with validation and deduplication"""
        logger.info(f"Processing DAM Bids: {file_path}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        processed_count = 0
        batch_data = []

        try:
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
                chunk.columns = normalize_headers(chunk.columns)
                for _, row in chunk.iterrows():
                    row_dict = normalize_dict_keys(row.to_dict())
                    try:
                        bid = DAMEnergyBid(**row_dict)
                        # Extract up to 10 price/MW pairs
                        for i in range(1, 11):
                            mw = getattr(bid, f"energy_only_bid_mw{i}", None)
                            price = getattr(
                                bid, f"energy_only_bid_price{i}", None)
                            if mw is not None and price is not None:
                                batch_data.append((
                                    bid.delivery_date,
                                    bid.hour_ending,
                                    bid.settlement_point_name,
                                    bid.qse_name,
                                    str(bid.bid_id),
                                    bid.multi_hour_block,
                                    bid.block_curve,
                                    mw,
                                    price,
                                    i
                                ))
                                processed_count += 1
                    except Exception as e:
                        if processed_count < 10:  # Only log first 10 errors
                            logger.warning(f"Skipping invalid bid row: {e}")
                        continue

                    # Batch insert when we hit the batch size
                    if len(batch_data) >= BATCH_SIZE:
                        cursor.executemany('''
                            INSERT OR REPLACE INTO DAMENERGYBIDS (
                                DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                                BIDID, MULTIHOURBLOCK, BLOCKCURVE, ENERGYONLYBIDMW,
                                ENERGYONLYBIDPRICE, BIDSEGMENT
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', batch_data)
                        conn.commit()
                        batch_data = []

            # Insert any remaining data
            if batch_data:
                cursor.executemany('''
                    INSERT OR REPLACE INTO DAMENERGYBIDS (
                        DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                        BIDID, MULTIHOURBLOCK, BLOCKCURVE, ENERGYONLYBIDMW,
                        ENERGYONLYBIDPRICE, BIDSEGMENT
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch_data)
                conn.commit()
        finally:
            conn.close()
        logger.info(f"Processed {processed_count} DAM bids")

    def process_dam_offer_awards(self, file_path: str):
        """Process DAM Energy Offer Awards into separate table with validation and deduplication"""
        logger.info(f"Processing DAM Offer Awards: {file_path}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        processed_count = 0
        batch_data = []

        try:
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
                chunk.columns = normalize_headers(chunk.columns)
                for _, row in chunk.iterrows():
                    row_dict = normalize_dict_keys(row.to_dict())
                    try:
                        offer_award = DAMEnergyOfferAward(**row_dict)
                        batch_data.append((
                            offer_award.delivery_date,
                            offer_award.hour_ending,
                            offer_award.settlement_point_name,
                            offer_award.qse_name,
                            offer_award.energy_only_offer_award_in_mw,
                            offer_award.settlement_point_price,
                            offer_award.offer_id
                        ))
                        processed_count += 1
                    except Exception as e:
                        if processed_count < 10:  # Only log first 10 errors
                            logger.warning(
                                f"Skipping invalid offer award row: {e}")
                        continue

                    # Batch insert when we hit the batch size
                    if len(batch_data) >= BATCH_SIZE:
                        cursor.executemany('''
                            INSERT OR REPLACE INTO DAMENERGYOFFERAWARDS (
                                DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                                ENERGYONLYOFFERAWARDINMW, SETTLEMENTPOINTPRICE, OFFERID
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', batch_data)
                        conn.commit()
                        batch_data = []

            # Insert any remaining data
            if batch_data:
                cursor.executemany('''
                    INSERT OR REPLACE INTO DAMENERGYOFFERAWARDS (
                        DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                        ENERGYONLYOFFERAWARDINMW, SETTLEMENTPOINTPRICE, OFFERID
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', batch_data)
                conn.commit()
        finally:
            conn.close()
        logger.info(f"Processed {processed_count} DAM offer awards")

    def get_dam_offer_count(self) -> int:
        """Get the actual count of DAM offers in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM DAMENERGYOFFERS")
            count = cursor.fetchone()[0]
            conn.close()
            logger.debug(f"Current DAM offers count in database: {count}")
            return count
        except Exception as e:
            logger.error(f"Failed to get DAM offer count: {e}")
            return 0

    def insert_dam_offers_batch(self, offers: List[Dict]) -> int:
        """Insert a batch of DAM offers into the database with validation and deduplication"""
        if not offers:
            return 0
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            insert_query = """
            INSERT OR REPLACE INTO DAMENERGYOFFERS (
                DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                OFFERID, MULTIHOURBLOCK, BLOCKCURVE, ENERGYONLYOFFERMW,
                ENERGYONLYOFFERPRICE, OFFERSEGMENT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            batch_data = []
            for offer_data in offers:
                logger.debug(f"Normalizing Offer Data: {offer_data}")
                offer_data = normalize_dict_keys(offer_data)
                try:
                    logger.debug(f"Storing DAMEnergyOffer: {offer_data}")
                    offer = DAMEnergyOffer(**offer_data)
                    for i in range(1, 11):
                        mw = getattr(offer, f"energy_only_offer_mw{i}", None)

                        price = getattr(
                            offer, f"energy_only_offer_price{i}", None)
                        if mw is not None and price is not None:
                            batch_data.append((
                                offer.delivery_date,
                                offer.hour_ending,
                                offer.settlement_point_name,
                                offer.qse_name,
                                str(offer.offer_id),
                                offer.multi_hour_block,
                                offer.block_curve,
                                mw,
                                price,
                                i
                            ))
                except Exception as e:
                    logger.warning(f"Skipping invalid offer row: {e}")
                    continue
            if batch_data:
                cursor.executemany(insert_query, batch_data)
                conn.commit()
            conn.close()
            logger.debug(
                f"Successfully inserted batch of {len(batch_data)} DAM offer segments")
            return len(batch_data)
        except Exception as e:
            logger.error(f"Failed to insert DAM offers batch: {e}")
            return 0

    def process_dam_offers(self, dam_offer_file: str, tracked_qses: set, dam_date: datetime):
        """Process DAM Energy Offers into separate table with validation and deduplication"""
        logger.info("Processing DAM Offers: %s", dam_offer_file)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        processed_count = 0
        batch_data = []
        try:
            for chunk in pd.read_csv(dam_offer_file, chunksize=CHUNK_SIZE):
                chunk.columns = normalize_headers(chunk.columns)
                for _, row in chunk.iterrows():
                    row_dict = normalize_dict_keys(row.to_dict())
                    try:
                        offer = DAMEnergyOffer(**row_dict)
                        for i in range(1, 11):
                            mw = getattr(
                                offer, f"energy_only_offer_mw{i}", None)
                            price = getattr(
                                offer, f"energy_only_offer_price{i}", None)
                            if mw is not None and price is not None:
                                batch_data.append((
                                    offer.delivery_date,
                                    offer.hour_ending,
                                    offer.settlement_point_name,
                                    offer.qse_name,
                                    str(offer.offer_id),
                                    offer.multi_hour_block,
                                    offer.block_curve,
                                    mw,
                                    price,
                                    i
                                ))
                                processed_count += 1
                    except Exception as e:
                        if processed_count < 10:
                            logger.warning("Skipping invalid offer row: %s", e)
                        continue
                    if len(batch_data) >= BATCH_SIZE:
                        cursor.executemany(
                            '''INSERT OR REPLACE INTO DAMENERGYOFFERS (
                                DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                                OFFERID, MULTIHOURBLOCK, BLOCKCURVE, ENERGYONLYOFFERMW,
                                ENERGYONLYOFFERPRICE, OFFERSEGMENT
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            batch_data
                        )
                        conn.commit()
                        batch_data = []
            if batch_data:
                cursor.executemany(
                    '''INSERT OR REPLACE INTO DAMENERGYOFFERS (
                        DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                        OFFERID, MULTIHOURBLOCK, BLOCKCURVE, ENERGYONLYOFFERMW,
                        ENERGYONLYOFFERPRICE, OFFERSEGMENT
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    batch_data
                )
                conn.commit()
        finally:
            conn.close()
        logger.info("Processed %d DAM offers", processed_count)

    def ensure_dam_metadata_flags(self):
        """Ensure DAM metadata table has all processing flag columns."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for col in ["BIDSEXTRACTED", "BIDAWARDSEXTRACTED", "OFFERSEXTRACTED", "OFFERAWARDSEXTRACTED"]:
            try:
                cursor.execute(
                    f"ALTER TABLE DAMARCHIVESMETADATA ADD COLUMN {col} INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                # Column already exists
                pass
        conn.commit()
        conn.close()

    def ensure_spp_metadata_flag(self):
        """Ensure SPP metadata table has the extracted flag column."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "ALTER TABLE spp_documents_metadata ADD COLUMN extracted INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def update_dam_flag(self, doc_id, flag, conn=None):
        """Update DAM flag with optional connection reuse"""
        close_conn = conn is None
        if conn is None:
            conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE DAMARCHIVESMETADATA SET {flag}=1 WHERE doc_id=?", (doc_id,))
        conn.commit()
        if close_conn:
            conn.close()

    def update_spp_flag(self, doc_id, conn=None):
        """Update SPP flag with optional connection reuse"""
        close_conn = conn is None
        if conn is None:
            conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE SPPARCHIVESMETADATA SET extracted=1 WHERE doc_id=?", (doc_id,))
        conn.commit()
        if close_conn:
            conn.close()

    def process_dam_archives(self, temp_dir):
        self.ensure_dam_metadata_flags()

        # Reuse connection for flag updates
        flag_conn = sqlite3.connect(self.db_path)

        try:
            cursor = flag_conn.cursor()
            cursor.execute(
                "SELECT DOCID, DOWNLOADURL, BIDS, BIDAWARDSEXTRACTED, OFFERSEXTRACTED, OFFERAWARDSEXTRACTED FROM DAMARCHIVESMETADATA")
            docs = cursor.fetchall()

            for doc_id, download_url, bids_flag, bid_awards_flag, offers_flag, offer_awards_flag in docs:
                file_path = os.path.join(temp_dir, f"{doc_id}.zip")
                if not all([bids_flag, bid_awards_flag, offers_flag, offer_awards_flag]):
                    # Download if needed
                    if not os.path.exists(file_path):
                        logger.info(f"Downloading DAM archive doc_id={doc_id}")
                        headers = self.get_bearer_headers()
                        response = self.session.get(
                            download_url, stream=True, headers=headers)
                        response.raise_for_status()
                        with open(file_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                    # Extract files
                    extracted_files = self.extract_dam_files(
                        file_path, temp_dir)
                    # Process each file type if not already done (reuse connection for flags)
                    if not bids_flag and '60d_DAM_EnergyBids' in extracted_files:
                        self.process_dam_bids(
                            extracted_files['60d_DAM_EnergyBids'])
                        self.update_dam_flag(
                            doc_id, 'BIDSEXTRACTED', flag_conn)
                    if not bid_awards_flag and '60d_DAM_EnergyBidAwards' in extracted_files:
                        self.process_dam_bid_awards(
                            extracted_files['60d_DAM_EnergyBidAwards'])
                        self.update_dam_flag(
                            doc_id, 'BIDAWARDSEXTRACTED', flag_conn)
                    if not offers_flag and '60d_DAM_EnergyOnlyOffers' in extracted_files:
                        self.process_dam_offers(
                            extracted_files['60d_DAM_EnergyOnlyOffers'], self.tracking_qse_short_names, None)
                        self.update_dam_flag(
                            doc_id, 'OFFERSEXTRACTED', flag_conn)
                    if not offer_awards_flag and '60d_DAM_EnergyOnlyOfferAwards' in extracted_files:
                        self.process_dam_offer_awards(
                            extracted_files['60d_DAM_EnergyOnlyOfferAwards'])
                        self.update_dam_flag(
                            doc_id, 'OFFERAWARDSEXTRACTED', flag_conn)
        finally:
            flag_conn.close()

    def process_spp_archives(self, temp_dir):
        self.ensure_spp_metadata_flag()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT doc_id, download_url, extracted FROM spp_documents_metadata")
        docs = cursor.fetchall()
        conn.close()

        # Get settlement points from both awards tables (union)
        conn = sqlite3.connect(self.db_path)
        bid_points = set(row[0] for row in conn.execute(
            "SELECT DISTINCT SETTLEMENTPOINTNAME FROM DAMENERGYBIDAWARDS"))
        offer_points = set(row[0] for row in conn.execute(
            "SELECT DISTINCT SETTLEMENTPOINTNAME FROM DAMENERGYOFFERAWARDS"))
        all_points = bid_points | offer_points
        conn.close()

        for doc_id, download_url, extracted_flag in docs:
            if not extracted_flag:
                file_path = os.path.join(temp_dir, f"{doc_id}.zip")
                logger.info(f"Downloading SPP doc_id={doc_id}")
                headers = self.get_bearer_headers()
                response = self.session.get(
                    download_url, stream=True, headers=headers)
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            # Process SPP file, filtering by settlement points
                            f.write(chunk)
                self.process_spp_data(file_path, all_points)
                self.update_spp_flag(doc_id)

    def process_spp_data(self, file_path, offers_points):
        import pandas as pd
        logger.debug(f"Processing SPP file: {file_path}")

        # Normalize offers_points once outside the loop for performance
        def normalize(s):
            return re.sub(r'[^A-Za-z0-9]', '', str(s)).upper()

        norm_offers_points = set(normalize(pt) for pt in offers_points)

        # Open DB connection once for the entire file
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
                # Normalize headers and columns
                chunk.columns = normalize_headers(chunk.columns)

                # Vectorized settlement point filtering - much faster than apply()
                if "SETTLEMENTPOINTNAME" in chunk.columns and len(chunk) > 0:
                    # Normalize settlement point names vectorized
                    chunk["NORM_SPT"] = chunk["SETTLEMENTPOINTNAME"].astype(
                        str).str.replace(r'[^A-Za-z0-9]', '', regex=True).str.upper()
                    filtered = chunk[chunk["NORM_SPT"].isin(
                        norm_offers_points)]

                    if len(filtered) > 0:
                        # Aggregate by DeliveryDate, DeliveryHour, SettlementPointName, SettlementPointType
                        grouped = (
                            filtered.groupby([
                                "DELIVERYDATE", "DELIVERYHOUR", "SETTLEMENTPOINTNAME", "SETTLEMENTPOINTTYPE"
                            ], as_index=False)["SETTLEMENTPOINTPRICE"].mean()
                        )

                        # Batch insert data
                        if len(grouped) > 0:
                            batch_data = [
                                (
                                    row["DELIVERYDATE"],
                                    row["DELIVERYHOUR"],
                                    row["SETTLEMENTPOINTNAME"],
                                    row["SETTLEMENTPOINTTYPE"],
                                    row["SETTLEMENTPOINTPRICE"],
                                    None,  # dstFlag, if available
                                )
                                for _, row in grouped.iterrows()
                            ]
                            cursor.executemany('''
                                INSERT OR REPLACE INTO SETTLEMENTPOINTPRICES (
                                    DELIVERYDATE, DELIVERYHOUR, SETTLEMENTPOINT, SETTLEMENTPOINTTYPE, AVGSETTLEMENTPOINTPRICE, DSTFLAG
                                ) VALUES (?, ?, ?, ?, ?, ?)
                            ''', batch_data)

            conn.commit()
        finally:
            conn.close()

    def run_pipeline(self, spp_start_date, spp_end_date):
        """Run the complete ETL pipeline in batches by SPP date, with DAM offset (+60 days)"""
        start_time = time.time()
        logger.info(
            f"Starting ERCOT data pipeline for SPP dates {spp_start_date} to {spp_end_date}")
        logger.info(
            f"Tracking {len(self.tracking_qse_short_names)} QSEs from ERCOT_tracking_list.csv")
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Get all archives with pagination
                logger.info("Fetching all DAM archives...")
                dam_archives = self.get_all_dam_archives()
                dam_archives = self.normalize_dicts_in_list(dam_archives)

                logger.info("Fetching all SPP documents...")
                spp_docs_all = self.get_all_spp_documents()
                spp_docs_all = self.normalize_dicts_in_list(spp_docs_all)

                total_days = (spp_end_date - spp_start_date).days + 1
                processed_days = 0
                for i in range(total_days):
                    spp_date = spp_start_date + timedelta(days=i)
                    dam_date = spp_date + timedelta(days=60+i)
                    logger.info(
                        f"\n=== Processing SPP date: {spp_date.date()} (DAM date: {dam_date.date()}) ===")
                    dam_archive = self.find_dam_archive_for_date(
                        dam_archives, dam_date)
                    if not dam_archive:
                        logger.warning(
                            f"Skipping SPP date {spp_date.date()} due to missing DAM archive for {dam_date.date()}")
                        continue
                    archive_path = self.download_dam_archive(
                        str(dam_archive['DOCID']), temp_dir)
                    logger.info(
                        f"Extracting DAM files for {dam_date.date()} from {archive_path}")
                    extracted_files = self.extract_dam_files(
                        archive_path, temp_dir)
                    # Step 2: Process DAM files sequentially
                    logger.debug(f"Extracted DAM files: {extracted_files}")
                    dam_files_processed = 0

                    if '60d_DAM_EnergyBidAwards' in extracted_files:
                        logger.info(
                            f"Processing DAM Bid Awards for {dam_date.date()}")
                        try:
                            self.process_dam_bid_awards(
                                extracted_files['60d_DAM_EnergyBidAwards'])
                            dam_files_processed += 1
                        except Exception as e:
                            logger.error(
                                f"Failed to process DAM Bid Awards: {e}")
                    else:
                        logger.warning(
                            f"Missing 60d_DAM_EnergyBidAwards for {dam_date.date()}")

                    if '60d_DAM_EnergyOnlyOfferAwards' in extracted_files:
                        logger.info(
                            f"Processing DAM Offer Awards for {dam_date.date()}")
                        try:
                            self.process_dam_offer_awards(
                                extracted_files['60d_DAM_EnergyOnlyOfferAwards'])
                            dam_files_processed += 1
                        except Exception as e:
                            logger.error(
                                f"Failed to process DAM Offer Awards: {e}")
                    else:
                        logger.warning(
                            f"Missing 60d_DAM_EnergyOnlyOfferAwards for {dam_date.date()}")

                    if '60d_DAM_EnergyBids' in extracted_files:
                        logger.info(
                            f"Processing DAM Bids for {dam_date.date()}")
                        try:
                            self.process_dam_bids(
                                extracted_files['60d_DAM_EnergyBids'])
                            dam_files_processed += 1
                        except Exception as e:
                            logger.error(f"Failed to process DAM Bids: {e}")
                    else:
                        logger.warning(
                            f"Missing 60d_DAM_EnergyBids for {dam_date.date()}")

                    if '60d_DAM_EnergyOnlyOffers' in extracted_files:
                        logger.info(
                            f"Processing DAM Offers for {dam_date.date()}")
                        try:
                            self.process_dam_offers(
                                extracted_files['60d_DAM_EnergyOnlyOffers'],
                                self.tracking_qse_short_names,
                                dam_date
                            )
                            dam_files_processed += 1
                        except Exception as e:
                            logger.error(f"Failed to process DAM Offers: {e}")
                    else:
                        logger.warning(
                            f"Missing 60d_DAM_EnergyOnlyOffers for {dam_date.date()}")

                    if dam_files_processed > 0:
                        logger.info(
                            f"Successfully processed {dam_files_processed} DAM files for {dam_date.date()}")
                    else:
                        logger.warning(
                            f"No DAM files processed for {dam_date.date()}")
                    spp_docs = self.find_spp_docs_for_date(
                        spp_docs_all, spp_date)
                    spp_docs = self.normalize_dicts_in_list(spp_docs)
                    if not spp_docs:
                        logger.warning(
                            f"No SPP docs found for {spp_date.date()}")
                    else:
                        # Load settlement points from DAM offers for filtering SPP data
                        conn = sqlite3.connect(self.db_path)
                        cursor = conn.cursor()
                        offers_points = set(row[0] for row in cursor.execute(
                            "SELECT DISTINCT SETTLEMENTPOINTNAME FROM DAMENERGYOFFERS"))
                        conn.close()

                        # Process SPP documents sequentially
                        spp_files_processed = 0
                        for spp_doc in spp_docs:
                            try:
                                spp_doc_id = spp_doc['DOCID']
                                friendly_name = spp_doc.get('FRIENDLYNAME', '')
                                logger.info(
                                    f"Downloading SPP doc: {friendly_name} (docId={spp_doc_id})")
                                spp_path = self.download_spp_data(
                                    str(spp_doc_id), temp_dir)

                                logger.info(
                                    f"Processing SPP data for {spp_date.date()}")
                                self.process_spp_data(spp_path, offers_points)
                                spp_files_processed += 1

                            except Exception as e:
                                logger.error(
                                    f"Failed to process SPP document {spp_doc_id}: {e}")

                        if spp_files_processed > 0:
                            logger.info(
                                f"Successfully processed {spp_files_processed} SPP files for {spp_date.date()}")
                        else:
                            logger.warning(
                                f"No SPP files processed for {spp_date.date()}")
                    # Step 4: Merge into FINAL table (no deletion)
                    logger.info(
                        f"Merging data for SPP date {spp_date.date()} (DAM date {dam_date.date()}) into FINAL table")
                    self.create_final_table(spp_date, dam_date)
                    processed_days += 1
                    logger.info(
                        f"=== Finished processing SPP date {spp_date.date()} ===\n")
                elapsed_time = time.time() - start_time
                logger.info(
                    f"Batch pipeline completed: {processed_days}/{total_days} days processed.")
                logger.info(
                    f"Total processing time: {elapsed_time:.2f} seconds")
            except Exception as e:
                logger.error(f"Pipeline failed: {e}")
                raise

    def find_dam_archive_for_date(self, dam_archives: List[Dict], target_date: datetime) -> Optional[Dict]:
        """Find DAM archive for specific date using both POSTDATETIME and DATEEXTRACTED fields."""
        target_date_str = target_date.strftime("%Y-%m-%d")
        logger.debug(
            f"Looking for DAM archive for date: {target_date_str} using both POSTDATETIME and DATEEXTRACTED"
        )
        logger.debug("archive files (head): " + str(dam_archives[:5]))

        # Try to match by POSTDATETIME date part (YYYY-MM-DD)
        for archive in dam_archives:
            post_datetime = archive.get('POSTDATETIME')
            if post_datetime:
                # Handle full ISO format (2010-12-03T10:47:13.000) and extract date part
                post_date = post_datetime.split(
                    'T')[0] if 'T' in post_datetime else post_datetime[:10]
                if post_date == target_date_str:
                    logger.debug(
                        f"Found DAM archive by POSTDATETIME: {archive.get('FRIENDLYNAME', '')} ({archive.get('DOCID', '')})"
                    )
                    return archive

        # Try to match by DATEEXTRACTED field (YYYY-MM-DD format)
        for archive in dam_archives:
            date_extracted = archive.get('DATEEXTRACTED')
            if date_extracted:
                # Handle both full ISO format and date-only format
                extracted_date = date_extracted.split(
                    'T')[0] if 'T' in date_extracted else date_extracted[:10]
                if extracted_date == target_date_str:
                    logger.debug(
                        f"Found DAM archive by DATEEXTRACTED: {archive.get('FRIENDLYNAME', '')} ({archive.get('DOCID', '')})"
                    )
                    # Fallback: try matching by friendlyName pattern (if any date is present)
                    return archive
        target_friendly_format = target_date.strftime("%d-%b-%y").upper()
        for archive in dam_archives:
            if target_friendly_format in archive.get('FRIENDLYNAME', ''):
                logger.debug(
                    f"Found DAM archive by filename pattern: {archive.get('FRIENDLYNAME', '')}"
                )
                return archive

        logger.warning(f"No DAM archive found for {target_date_str}")
        return None

    def find_spp_docs_for_date(self, spp_documents: List[Dict], target_date: datetime) -> List[Dict]:
        """Find SPP documents for specific date using both POSTDATETIME and DATEEXTRACTED fields."""
        target_date_str = target_date.strftime("%Y-%m-%d")
        target_format = target_date.strftime("%Y%m%d")
        logger.debug(
            f"Looking for SPP docs for date: {target_date_str} (format: {target_format}) using both POSTDATETIME and DATEEXTRACTED"
        )

        matching_docs = []

        # Try to match by POSTDATETIME date part (YYYY-MM-DD)
        for doc in spp_documents:
            post_datetime = doc.get('POSTDATETIME')
            if post_datetime:
                # Handle full ISO format (2010-12-03T10:47:13.000) and extract date part
                post_date = post_datetime.split(
                    'T')[0] if 'T' in post_datetime else post_datetime[:10]
                if post_date == target_date_str:
                    matching_docs.append(doc)

        # Try to match by DATEEXTRACTED field (YYYY-MM-DD format)
        for doc in spp_documents:
            date_extracted = doc.get('DATEEXTRACTED')
            if date_extracted:
                # Handle both full ISO format and date-only format
                extracted_date = date_extracted.split(
                    'T')[0] if 'T' in date_extracted else date_extracted[:10]
                if extracted_date == target_date_str and doc not in matching_docs:
                    matching_docs.append(doc)

        # If no exact matches, try filename pattern matching
        if not matching_docs:
            for doc in spp_documents:
                if target_format in doc.get('FRIENDLYNAME', ''):
                    matching_docs.append(doc)

        logger.debug(
            f"Found {len(matching_docs)} SPP documents for {target_date_str}"
        )
        return matching_docs

    def create_final_table(self, spp_date, dam_date):
        """
        Create final merged table from individual tables (no deletion), only for the current SPP and DAM date.
        """
        logger.info(
            f"Creating final merged table for SPP date {spp_date.date()} and DAM date {dam_date.date()} (no deletion)"
        )
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Insert bid awards with SPP data for the current DAM/SPP date only
        cursor.execute(
            '''
            INSERT OR REPLACE INTO FINAL (
                DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                SETTLEMENTPOINTPRICE, AVGSETTLEMENTPOINTPRICE, BLOCKCURVE, SOURCETYPE,
                ENERGYONLYBIDAWARDINMW, BIDID, ENERGYONLYOFFERAWARDMW, OFFERID
            )
            SELECT
                BA.DELIVERYDATE,
                BA.HOURENDING,
                BA.SETTLEMENTPOINTNAME,
                BA.QSENAME,
                BA.SETTLEMENTPOINTPRICE,
                SPP.AVGSETTLEMENTPOINTPRICE,
                NULL as BLOCKCURVE,
                'BID_AWARD' as SOURCETYPE,
                BA.ENERGYONLYBIDAWARDINMW,
                BA.BIDID,
                NULL as ENERGYONLYOFFERAWARDMW,
                NULL as OFFERID
            FROM DAMENERGYBIDAWARDS BA
            LEFT JOIN SETTLEMENTPOINTPRICES SPP
                ON BA.DELIVERYDATE = SPP.DELIVERYDATE
                AND BA.HOURENDING = SPP.DELIVERYHOUR + 1
                AND BA.SETTLEMENTPOINTNAME = SPP.SETTLEMENTPOINT
            WHERE BA.DELIVERYDATE = ?
            ''',
            (dam_date.strftime('%Y-%m-%d'),)
        )
        logger.debug("Merged BID_AWARD rows into FINAL for date %s: %d",
                     dam_date.strftime('%Y-%m-%d'), cursor.rowcount)
        # Insert offer awards with SPP data for the current DAM/SPP date only
        cursor.execute(
            '''
            INSERT OR REPLACE INTO FINAL (
                DELIVERYDATE, HOURENDING, SETTLEMENTPOINTNAME, QSENAME,
                SETTLEMENTPOINTPRICE, AVGSETTLEMENTPOINTPRICE, BLOCKCURVE, SOURCETYPE,
                ENERGYONLYBIDAWARDINMW, BIDID, ENERGYONLYOFFERAWARDMW, OFFERID
            )
            SELECT
                OA.DELIVERYDATE,
                OA.HOURENDING,
                OA.SETTLEMENTPOINTNAME,
                OA.QSENAME,
                OA.SETTLEMENTPOINTPRICE,
                SPP.AVGSETTLEMENTPOINTPRICE,
                NULL as BLOCKCURVE,
                'OFFERAWARD' as SOURCETYPE,
                NULL as ENERGYONLYBIDAWARDINMW,
                NULL as BIDID,
                OA.ENERGYONLYOFFERAWARDINMW,
                OA.OFFERID
            FROM DAMENERGYOFFERAWARDS OA
            LEFT JOIN SETTLEMENTPOINTPRICES SPP
                ON OA.DELIVERYDATE = SPP.DELIVERYDATE
                AND OA.HOURENDING = SPP.DELIVERYHOUR + 1
                AND OA.SETTLEMENTPOINTNAME = SPP.SETTLEMENTPOINT
            WHERE OA.DELIVERYDATE = ?
            ''',
            (dam_date.strftime('%Y-%m-%d'),)
        )
        logger.debug("Merged OFFERAWARD rows into FINAL for date %s: %d",
                     dam_date.strftime('%Y-%m-%d'), cursor.rowcount)
        conn.commit()
        # Get final counts for this day
        cursor.execute(
            "SELECT COUNT(*) FROM FINAL WHERE DELIVERYDATE = ?",
            (dam_date.strftime('%Y-%m-%d'),)
        )
        final_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(DISTINCT QSENAME) FROM FINAL WHERE DELIVERYDATE = ?",
            (dam_date.strftime('%Y-%m-%d'),)
        )
        unique_qses = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(DISTINCT SETTLEMENTPOINTNAME) FROM FINAL WHERE DELIVERYDATE = ?",
            (dam_date.strftime('%Y-%m-%d'),)
        )
        unique_settlement_points = cursor.fetchone()[0]
        conn.close()
        logger.info(
            f"Final table updated for {dam_date.date()} with {final_count:,} records"
        )
        logger.info(f"Unique QSEs: {unique_qses}")
        logger.info(f"Unique settlement points: {unique_settlement_points}")

    def is_metadata_cache_fresh(self, cache_type: str, max_age_hours: int = 24) -> bool:
        """Check if metadata cache is fresh enough to use by checking CACHEDATE in archive tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Determine which table to check based on cache_type
            table_name = "DAMARCHIVESMETADATA" if cache_type == "dam_archives" else "SPPARCHIVESMETADATA"

            # Get the most recent CACHEDATE from the appropriate table
            cursor.execute(f"""
                SELECT MAX(CACHEDATE)
                FROM {table_name}
                WHERE CACHEDATE IS NOT NULL
            """)

            result = cursor.fetchone()
            conn.close()

            if not result or not result[0]:
                logger.debug(f"No cache date found for {cache_type}")
                return False

            last_updated_str = result[0]
            last_updated = datetime.fromisoformat(last_updated_str)
            age = datetime.now() - last_updated

            is_fresh = age.total_seconds() < (max_age_hours * 3600)
            logger.debug(
                f"Cache {cache_type}: last_updated={last_updated}, age={age}, fresh={is_fresh}")
            return is_fresh

        except Exception as e:
            logger.error(
                f"Error checking cache freshness for {cache_type}: {e}")
            return False

    def load_metadata_from_cache(self, cache_type: str) -> List[Dict]:
        """Load metadata from database cache"""
        try:
            table_name = "DAMARCHIVESMETADATA" if cache_type == "dam_archives" else "SPPARCHIVESMETADATA"
            logger.debug(f"Loading metadata from cache for {cache_type}")
            logger.debug(f"Using table: {table_name}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT DOCID, FRIENDLYNAME, POSTDATETIME, DOWNLOADURL, DATEEXTRACTED
                FROM {table_name}
                ORDER BY POSTDATETIME DESC
            """)

            results = []
            for row in cursor.fetchall():
                doc_id, friendly_name, post_datetime, download_url, date_extracted = row
                results.append(normalize_dict_keys({
                    'docId': doc_id,
                    'friendlyName': friendly_name,
                    'postDatetime': post_datetime,
                    'date_extracted': date_extracted,
                    '_links': {'download': download_url}}))
            conn.close()
            return results
        except Exception as e:
            logger.error(f"Failed to load metadata from cache: {e}")
            return []

    def save_metadata_to_cache(self, cache_type: str, metadata_list: List[Dict]):
        """Save metadata to database cache with deduplication by doc_id and INSERT OR REPLACE."""
        try:
            logger.debug(
                f"Saving metadata to cache for {cache_type}: {metadata_list}")
            logger.debug(f"Metadata count: {len(metadata_list)}")
            logger.debug(f"Metadata sample: {metadata_list[:5]}")
            table_name = "DAMARCHIVESMETADATA" if cache_type == "dam_archives" else "SPPARCHIVESMETADATA"
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            deduped = {}
            for item in metadata_list:
                item = normalize_dict_keys(item)
                doc_id = item['DOCID'] if 'DOCID' in item else item.get(
                    'docId')
                if doc_id not in deduped:
                    deduped[doc_id] = item
                else:
                    logger.debug(
                        f"Duplicate doc_id {doc_id} found in {cache_type}, skipping duplicate.")
            cached_at = datetime.now().isoformat()
            batch_data = []
            for item in deduped.values():
                doc_id = item['DOCID']
                friendly_name = item['FRIENDLYNAME']
                post_datetime = item['POSTDATETIME']
                download_url = item['LINKS']['ENDPOINT']['HREF']
                # Parse DATEEXTRACTED from POSTDATETIME (extract date part: YYYY-MM-DD)
                try:
                    # Parse POSTDATETIME format like "2010-12-03T10:47:13.000" and extract date part
                    if post_datetime and 'T' in post_datetime:
                        date_extracted = post_datetime.split(
                            'T')[0]  # Extract YYYY-MM-DD
                    else:
                        # Fallback to current date if POSTDATETIME is malformed
                        date_extracted = datetime.now().strftime('%Y-%m-%d')
                        logger.warning(
                            f"Malformed POSTDATETIME '{post_datetime}' for doc_id {doc_id}, using current date")
                except Exception as e:
                    date_extracted = datetime.now().strftime('%Y-%m-%d')
                    logger.warning(
                        f"Failed to parse POSTDATETIME '{post_datetime}' for doc_id {doc_id}: {e}")

                batch_data.append(
                    (doc_id, friendly_name, post_datetime, download_url, cached_at, date_extracted))
            cursor.executemany(f"""
                INSERT OR REPLACE INTO {table_name} (
                    DOCID, FRIENDLYNAME, POSTDATETIME, DOWNLOADURL, CACHEDATE, DATEEXTRACTED
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, batch_data)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to save metadata to cache: {e}")

    def setup_metadata_tables(self):
        """Create metadata tables for DAM and SPP archives if they do not exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DAMARCHIVESMETADATA (
                DOCID INTEGER PRIMARY KEY,
                FRIENDLYNAME TEXT,
                POSTDATETIME TEXT,
                DATEINDEX TEXT,
                DOWNLOADURL TEXT,
                RAWJSON TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS SPPARCHIVESMETADATA (
                DOCID INTEGER PRIMARY KEY,
                FRIENDLYNAME TEXT,
                POSTDATETIME TEXT,
                DATEINDEX TEXT,
                DOWNLOADURL TEXT,
                RAWJSON TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def setup_metadata_tables_with_flags(self):
        """Ensure metadata tables have status flags for downstream data loads."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # DAM metadata flags
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DAMARCHIVESMETADATA (
                DOCID INTEGER PRIMARY KEY,
                FRIENDLYNAME TEXT,
                POSTDATETIME TEXT,
                DATEINDEX TEXT,
                DOWNLOADURL TEXT,
                RAWJSON TEXT,
                BIDSLOADED INTEGER DEFAULT 0,
                BIDAWARDSLOADED INTEGER DEFAULT 0,
                OFFERSLOADED INTEGER DEFAULT 0,
                OFFERAWARDSLOADED INTEGER DEFAULT 0
            )
        ''')
        # SPP metadata flags
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS SPPARCHIVESMETADATA (
                DOCID INTEGER PRIMARY KEY,
                FRIENDLYNAME TEXT,
                POSTDATETIME TEXT,
                DATEINDEX TEXT,
                DOWNLOADURL TEXT,
                RAWJSON TEXT,
                SETTLEMENTPOINTPRICESLOADED INTEGER DEFAULT 0
            )
        ''')
        # Add columns if missing (idempotent)
        for col in ["BIDSLOADED", "BIDAWARDSLOADED", "OFFERSLOADED", "OFFERAWARDSLOADED"]:
            try:
                cursor.execute(
                    f"ALTER TABLE DAMARCHIVESMETADATA ADD COLUMN {col} INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
        try:
            cursor.execute(
                "ALTER TABLE SPPARCHIVESMETADATA ADD COLUMN SETTLEMENTPOINTPRICESLOADED INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def store_metadata(self, product: str, archives: list):
        """Store metadata for DAM or SPP archives in the appropriate table."""
        table = 'DAMARCHIVESMETADATA' if product == 'DAM' else 'SPPARCHIVESMETADATA'
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for archive in archives:
            doc_id = archive.get('DOCID')
            friendly_name = archive.get('FRIENDLYNAME')
            post_datetime = archive.get('POSTDATETIME')
            # Extract date_index from post_datetime (YYYY-MM-DD)
            date_index = None
            if post_datetime:
                try:
                    date_index = datetime.strptime(
                        post_datetime[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
                except Exception:
                    date_index = post_datetime[:10]
            download_url = archive.get('LINKS', {}).get(
                'ENDPOINT', {}).get('HREF')
            raw_json = json.dumps(archive)
            cursor.execute(f'''
                INSERT OR REPLACE INTO {table} (DOCID, FRIENDLYNAME, POSTDATETIME, DATEINDEX, DOWNLOADURL, RAWJSON)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (doc_id, friendly_name, post_datetime, date_index, download_url, raw_json))
        conn.commit()
        conn.close()

    def lookup_metadata(self, product: str, date_index: str):
        """Lookup doc_id and download_url for a given date_index in the metadata table."""
        table = 'DAMARCHIVESMETADATA' if product == 'DAM' else 'SPPARCHIVESMETADATA'
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f'''
            SELECT DOCID, DOWNLOADURL FROM {table} WHERE DATEINDEX = ?
        ''', (date_index,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return {'DOCID': row[0], 'DOWNLOADURL': row[1]}
        return None

    def refresh_metadata(self, product: str):
        """Fetch all pages of metadata from ERCOT API and store in the appropriate table."""
        if product == 'DAM':
            url = f"{ERCOT_API_BASE}/archive/NP3-966-er"
        else:
            url = f"{ERCOT_API_BASE}/archive/np6-905-cd"
        page = 1
        page_size = 1000
        all_archives = []
        total_pages = None
        while True:
            params = {'page': page, 'size': page_size}
            response = self.make_request_with_retry(url, params)
            data = response.json()
            if total_pages is None:
                meta = data.get('_meta', {})
                total_pages = meta.get('totalPages', 1)
                logger.info(
                    f"{product} Metadata: {meta.get('totalRecords', '?')} records across {total_pages} pages")
            archives = data.get('archives', [])
            logger.info(
                f"{product} Metadata: Retrieved {len(archives)} archives from page {page}/{total_pages}")
            if not archives:
                break
            all_archives.extend(archives)
            if len(archives) < page_size or page >= total_pages:
                break
            page += 1
        self.store_metadata(product, all_archives)
        return all_archives

    def get_or_refresh_metadata(self, product: str, date_index: str, freshness_hours: int = 24):
        """Get metadata for a date, refreshing if not found or stale."""
        self.setup_metadata_tables()
        result = self.lookup_metadata(product, date_index)
        if result:
            return result
        logger.info(
            f"No cached metadata for {product} {date_index}, refreshing...")
        self.refresh_metadata(product)
        return self.lookup_metadata(product, date_index)

    def get_doc_id_and_url_for_date(self, product: str, date: datetime) -> Optional[Dict]:
        """Convenience function to get doc_id and download_url for a given product and date."""
        date_index = date.strftime("%Y-%m-%d")
        meta = self.get_or_refresh_metadata(product, date_index)
        if meta:
            return {'doc_id': meta[0], 'download_url': meta[1]}
        logger.warning(f"No {product} doc_id found for {date_index}")
        return None

# --- METADATA CACHING: Replace legacy get_all_dam_archives with new metadata system ---
    def get_dam_doc_id_and_url(self, dam_date: datetime) -> Optional[Dict]:
        """Get doc_id and download_url for DAM for a given date using metadata cache."""
        return self.get_doc_id_and_url_for_date('DAM', dam_date)

    def get_spp_doc_id_and_url(self, spp_date: datetime) -> Optional[Dict]:
        """Get doc_id and download_url for SPP for a given date using metadata cache."""
        return self.get_doc_id_and_url_for_date('SPP', spp_date)

    def get_bearer_headers(self):
        """
        Returns HTTP headers required for Bearer token authentication.

        If the current token is missing or expired, attempts to authenticate and obtain a new token.
        Raises a RuntimeError if authentication fails or a valid token cannot be obtained.

        Returns:
            dict: A dictionary containing the 'Authorization' header with the Bearer token and the
                  'Ocp-Apim-Subscription-Key' header.

        Raises:
            RuntimeError: If authentication fails or a valid token cannot be retrieved.
        """
        if self.token is None or self.is_token_expired():
            auth_success = self.authenticate()
            if not auth_success or self.token is None:
                logger.error(
                    "Authentication failed. Token is None after authenticate(). Check credentials and API response.")
                raise RuntimeError(
                    "Failed to authenticate and obtain Bearer token. Check credentials, API response, and token parsing.")
        return {
            "Authorization": f"Bearer {self.token.access_token}",
            "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY
        }

    def extract_dam_files(self, archive_path: str, extract_dir: str) -> dict:
        """Extract required CSV files from DAM archive and log all contents."""
        import zipfile
        required_files = [
            '60d_DAM_EnergyBidAwards',
            '60d_DAM_EnergyBids',
            '60d_DAM_EnergyOnlyOfferAwards',
            '60d_DAM_EnergyOnlyOffers'
        ]
        extracted_files = {}
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            for name in zip_ref.namelist():
                for req in required_files:
                    if req in name and name.endswith('.csv'):
                        extract_path = os.path.join(
                            extract_dir, os.path.basename(name))
                        with zip_ref.open(name) as src, open(extract_path, 'wb') as tgt:
                            tgt.write(src.read())
                        extracted_files[req] = extract_path
                        logger.info(f"Extracted {req} to {extract_path}")
        logger.debug(f"Extracted files: {extracted_files}")
        return extracted_files


def normalize_headers(headers):
    """
    Normalize a list or pandas Index of headers by removing underscores, spaces, and special characters, and capitalizing everything.
    """
    def normalize(s):
        return re.sub(r'[^A-Za-z0-9]', '', str(s)).upper()
    return [normalize(h) for h in headers]


def normalize_dict_keys(d):
    """
    Recursively normalize all keys in a dictionary by removing underscores, spaces, and special characters, and capitalizing everything.
    """
    def normalize(s):
        return re.sub(r'[^A-Za-z0-9]', '', str(s)).upper()
    if isinstance(d, dict):
        return {normalize(k): normalize_dict_keys(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [normalize_dict_keys(i) for i in d]
    else:
        return d


# --- PARALLEL PROCESSING WRAPPERS ---
# Removed - no longer using parallel processing

# --- END PARALLEL PROCESSING WRAPPERS ---

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="ERCOT Data Pipeline")
    parser.add_argument(
        '--db', type=str, default="ercot_data.db",
        help="Path to SQLite database")
    parser.add_argument('--checkpoint', type=str,
                        default="pipeline_checkpoint.json",
                        help="Path to checkpoint file")
    parser.add_argument('--spp_start_date', type=str,
                        required=True, help="SPP start date (YYYY-MM-DD)")
    parser.add_argument('--spp_end_date', type=str,
                        required=True, help="SPP end date (YYYY-MM-DD)")
    args = parser.parse_args()

    pipeline = ERCOTDataPipeline(
        db_path=args.db, checkpoint_file=args.checkpoint)
    spp_start_date = datetime.strptime(args.spp_start_date, "%Y-%m-%d")
    spp_end_date = datetime.strptime(args.spp_end_date, "%Y-%m-%d")

    logger.info("Running in sequential mode")
    pipeline.run_pipeline(spp_start_date, spp_end_date)
