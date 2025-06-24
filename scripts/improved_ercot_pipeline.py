"""
Improved ERCOT Data Pipeline integrating existing authentication and metadata management
with new Pydantic models and concurrent processing
"""

# Standard library importsfrom pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Set, Optional, Dict, List
from dataclasses import dataclass
import zipfile
import io
from datetime import datetime
from datetime import timedelta
import asyncio
import json
import logging
import os
from pathlib import Path
from queue import Queue
import aiosqlite
import threading
import calendar
# Third-party imports
import sqlite3
import time
import tempfile
import requests
import pandas as pd
from dotenv import load_dotenv
# Local imports
from ercot_models import (
    ERCOTTrackingQSE,
    BatchProcessor,
    normalize_headers,
    ERCOTOpenApiClient
)

# API Configuration
ERCOT_AUTH_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
    "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
)
ERCOT_API_BASE = "https://api.ercot.com/api/public-reports"

# Add these product ID constants
ERCOT_PRODUCT_IDS = {
    'DAM': {
        'BUNDLE': 'NP3-966-ER',  # 60-Day DAM Disclosure Reports
    },
    'SPP': {
        'BUNDLE': 'NP6-905-cd',  # Settlement Point Prices at Resource Nodes, Hubs and Load Zones
    }
}

# Load environment variables
load_dotenv()

# Create module logger
logger = logging.getLogger(__name__)

# Load credentials from environment
SUBSCRIPTION_KEY = os.getenv("ERCOT_API_SUBSCRIPTION_KEY")
USERNAME = os.getenv("ERCOT_API_USERNAME")
PASSWORD = os.getenv("ERCOT_API_PASSWORD")

# Configuration
DOWNLOAD_WORKERS = 3
PROCESSING_WORKERS = 6
STORAGE_WORKERS = 4
RATE_LIMIT_DELAY = 2.0  # 2 seconds between API requests
MAX_QUEUE_SIZE = 1000
CHUNK_SIZE = 1_000
MAX_SQL_BATCH_SIZE = 999

if not all([USERNAME, PASSWORD, SUBSCRIPTION_KEY]):
    raise ValueError("Missing required environment variables")


@dataclass
class AuthToken:
    access_token: str
    expires_at: datetime


class RateLimiter:
    """Thread-safe rate limiter for API requests"""

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self.last_request_time = 0
        self.lock = threading.Lock()

    def wait_if_needed(self):
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                time.sleep(sleep_time)
            self.last_request_time = time.time()


class ImprovedERCOTDataPipeline:
    def __init__(self, db_path: str,
                 checkpoint_file: str = "pipeline_checkpoint.json",
                 base_log_dir: str = "logs", enable_cache: bool = True):
        # Load and validate subscription key
        if not SUBSCRIPTION_KEY:
            raise RuntimeError(
                "ERCOT API subscription key not set in environment")

        # Authenticate first to get bearer token
        if not self.authenticate():
            # Initialize OpenAPI client with both keys and auth callback
            raise RuntimeError(
                "Failed to authenticate with ERCOT API during initialization")
        self.ercot_api = ERCOTOpenApiClient(
            subscription_key=SUBSCRIPTION_KEY,
            bearer_token=self.token.access_token,
            auth_callback=self._refresh_token
        )

        self.db_path = db_path
        self.checkpoint_file = Path(checkpoint_file)
        self.base_log_dir = base_log_dir
        self.enable_cache = enable_cache
        self.cache_dir = Path("_cache")

        # Setup logging
        self.setup_logging()

        # Initialize queues
        self.download_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.processing_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.storage_queue = Queue(maxsize=MAX_QUEUE_SIZE)

        # Rate limiter
        self.rate_limiter = RateLimiter(RATE_LIMIT_DELAY)

        # Worker pools
        self.download_executor = ThreadPoolExecutor(
            max_workers=DOWNLOAD_WORKERS)
        self.processing_executor = ThreadPoolExecutor(
            max_workers=PROCESSING_WORKERS)
        self.storage_executor = ThreadPoolExecutor(max_workers=STORAGE_WORKERS)

        # Authentication
        self.token: Optional[AuthToken] = None
        self.token_expires_at = None
        self.session = requests.Session()
        self.min_request_interval = RATE_LIMIT_DELAY
        self.last_request_time = 0

        # Batch processor
        self.batch_processor = BatchProcessor()

        # Tracking data
        self.tracked_qses_df = self.load_tracking_qses()
        self.tracked_qse_names = set(
            self.tracked_qses_df['SHORTNAME'].str.upper()
        ) if not self.tracked_qses_df.empty else set()

        # Database setup with optimizations
        self.setup_optimized_database()

        # Setup metadata tables for cache tracking
        self.setup_metadata_tables()

        # Stats tracking
        self.stats = {
            'downloads_completed': 0,
            'processing_completed': 0,
            'storage_completed': 0,
            'errors': 0,
            'validation_errors': 0,
            "db_errors": 0,
        }
        self.stats_lock = threading.Lock()

        # Checkpoint data
        self.checkpoint_data = self.load_checkpoint()

        if self.enable_cache:
            self.cache_dir.mkdir(exist_ok=True)
            logger.info(f"Caching enabled - Cache directory: {self.cache_dir}")

    def setup_logging(self):
        """Setup structured logging with timestamped directories"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = Path(self.base_log_dir) / f"pipeline_run_{timestamp}"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Remove existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - '
            '%(funcName)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )

        # Setup file handlers
        log_files = {
            'debug': (logging.DEBUG, detailed_formatter),
            'info': (logging.INFO, simple_formatter),
            'error': (logging.ERROR, detailed_formatter),
        }

        for log_name, (level, formatter) in log_files.items():
            file_handler = logging.FileHandler(
                self.log_dir / f"{log_name}.log")
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logging.root.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        logging.root.addHandler(console_handler)

        logging.root.setLevel(logging.DEBUG)
        logger.info(f"Logging initialized - Log directory: {self.log_dir}")

    # Keep authentication logic from original file
    def _refresh_token(self):
        """Callback to refresh bearer token for API client"""
        try:
            if self.authenticate():
                return self.token.access_token
        except Exception:
            pass
        return None

    def authenticate(self) -> bool:
        """Authenticate with ERCOT API"""
        logger = logging.getLogger(__name__)

        logger.debug("Authenticating with ERCOT API...")
        auth_url = (
            f"{ERCOT_AUTH_URL}?username={USERNAME}&password={PASSWORD}"
            f"&grant_type=password"
            f"&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+"
            f"offline_access"
            f"&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70"
            f"&response_type=id_token"
        )

        try:
            response = requests.post(auth_url)
            response.raise_for_status()
            token_data = response.json()

            access_token = token_data.get("access_token")
            expires_in = int(token_data.get("expires_in", 3600))

            if access_token:
                self.token = AuthToken(
                    access_token=access_token,
                    expires_at=datetime.now() + timedelta(seconds=expires_in)
                )
                self.token_expires_at = self.token.expires_at
                logger.info("Authentication successful")
                return True
            else:
                logger.error("No access token in response")
                return False

        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def get_bearer_headers(self) -> Dict[str, str]:
        """Get headers with bearer token"""
        if self.token is None or self.is_token_expired():
            if not self.authenticate():
                raise RuntimeError("Failed to authenticate")

        return {
            "Authorization": f"Bearer {self.token.access_token}",
            "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY
        }

    def is_token_expired(self) -> bool:
        """Check if token is expired or about to expire"""
        if not self.token_expires_at:
            return True
        return datetime.now() >= (self.token.expires_at - timedelta(minutes=5))

    def wait_for_rate_limit(self):
        """Rate limiting implementation from original"""
        current_time = time.time()
        time_since_last = current_time - self.min_request_interval
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def make_request_with_retry(
        self, url: str, params: Dict = None,
        max_retries: int = 5
    ) -> requests.Response:
        """Request retry logic from original with rate limiting"""
        for attempt in range(max_retries):
            self.wait_for_rate_limit()
            try:
                headers = self.get_bearer_headers()
                response = self.session.get(
                    url, headers=headers, params=params)

                if response.status_code == 401:
                    if self.authenticate():
                        continue
                    response.raise_for_status()

                if response.status_code == 429:
                    wait_time = 60 if attempt > 0 else 2
                    logger.warning(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)

        raise Exception(
            f"Failed to complete request after {max_retries} attempts")

    def load_checkpoint(self) -> Dict:
        """Load checkpoint data"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

        return {
            'completed_dates': [],
            'failed_dates': [],
            'last_run_start': None,
            'last_run_end': None,
            'dam_archives_cache': {},
            'spp_documents_cache': {}
        }

    def save_checkpoint(self):
        """Save checkpoint data"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def load_tracking_qses(self) -> pd.DataFrame:
        """Load QSEs to track from CSV using pandas and Pydantic validation"""
        try:
            csv_path = Path("_data/ERCOT_tracking_list.csv")
            if not csv_path.exists():
                logger.error(f"Tracking QSE CSV file not found: {csv_path}")
                return pd.DataFrame()

            df = pd.read_csv(csv_path)
            df.columns = normalize_headers(df.columns)

            # Drop rows with NaN in any required string field for \
            # ERCOTTrackingQSE
            # Add more if your model requires more fields
            required_fields = ['SHORTNAME']
            df = df.dropna(subset=required_fields)

            # Replace NaN in optional web fields with None to prevent \
            # validation errors
            for col in ['WEB01', 'WEB02']:
                if col in df.columns:
                    # Replace any NaN or empty with None
                    df[col] = df[col].apply(
                        lambda v: None if (
                            isinstance(v, float) and pd.isna(v)
                        ) or (
                            isinstance(v, str) and v.strip().lower() == 'nan'
                        ) else v
                    )

            # Validate each row using Pydantic
            validated_rows = []
            for idx, row in df.iterrows():
                try:
                    qse = ERCOTTrackingQSE(**row.to_dict())
                    validated_rows.append(qse.model_dump())
                except Exception as e:
                    logger.warning(f"Invalid QSE row {idx}: {e}")
                    continue

            validated_df = pd.DataFrame(validated_rows)
            logger.info(
                f"Loaded and validated {len(validated_df)} tracked QSEs")
            return validated_df

        except Exception as e:
            logger.error(f"Failed to load tracking QSEs: {e}")
            return pd.DataFrame()

    def setup_optimized_database(self):
        """Setup database with performance optimizations"""
        conn = sqlite3.connect(self.db_path)

        # Enable optimizations
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")

        # Create tables with proper schema
        self._create_tables(conn)

        conn.close()

    def _create_tables(self, conn):
        """Create all necessary tables with indexes and wide format"""
        cursor = conn.cursor()

        # Main data tables (wide format) with DATE affinity for date columns
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS DAM_ENERGY_BID_AWARDS ('
            'DELIVERYDATE DATE, HOURENDING INTEGER, SETTLEMENTPOINTNAME TEXT, '
            'QSENAME TEXT, ENERGYONLYBIDAWARDINMW REAL, '
            'SETTLEMENTPOINTPRICE REAL, BIDID TEXT, '
            'INSERTEDAT TEXT'
            ')'
        )
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS DAM_ENERGY_BIDS ('
            'DELIVERYDATE DATE, HOURENDING INTEGER, SETTLEMENTPOINTNAME TEXT, '
            'QSENAME TEXT, BIDID TEXT, MULTIHOURBLOCK BOOLEAN, '
            'BLOCKCURVE BOOLEAN, '
            'ENERGYONLYBIDMW1 REAL, ENERGYONLYBIDPRICE1 REAL, '
            'ENERGYONLYBIDMW2 REAL, ENERGYONLYBIDPRICE2 REAL, '
            'ENERGYONLYBIDMW3 REAL, ENERGYONLYBIDPRICE3 REAL, '
            'ENERGYONLYBIDMW4 REAL, ENERGYONLYBIDPRICE4 REAL, '
            'ENERGYONLYBIDMW5 REAL, ENERGYONLYBIDPRICE5 REAL, '
            'ENERGYONLYBIDMW6 REAL, ENERGYONLYBIDPRICE6 REAL, '
            'ENERGYONLYBIDMW7 REAL, ENERGYONLYBIDPRICE7 REAL, '
            'ENERGYONLYBIDMW8 REAL, ENERGYONLYBIDPRICE8 REAL, '
            'ENERGYONLYBIDMW9 REAL, ENERGYONLYBIDPRICE9 REAL, '
            'ENERGYONLYBIDMW10 REAL, ENERGYONLYBIDPRICE10 REAL, '
            'INSERTEDAT TEXT DEFAULT (datetime(\'now\'))'
            ')'
        )
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS DAM_ENERGY_OFFER_AWARDS ('
            'DELIVERYDATE DATE, HOURENDING INTEGER, SETTLEMENTPOINTNAME TEXT, '
            'QSENAME TEXT, ENERGYONLYOFFERAWARDINMW REAL, '
            'SETTLEMENTPOINTPRICE REAL, OFFERID TEXT, '
            'INSERTEDAT TEXT'
            ')'
        )
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS DAM_ENERGY_OFFERS ('
            'DELIVERYDATE DATE, HOURENDING INTEGER, SETTLEMENTPOINTNAME TEXT, '
            'QSENAME TEXT, OFFERID TEXT, MULTIHOURBLOCK BOOLEAN, '
            'BLOCKCURVE BOOLEAN, '
            'ENERGYONLYOFFERMW1 REAL, ENERGYONLYOFFERPRICE1 REAL, '
            'ENERGYONLYOFFERMW2 REAL, ENERGYONLYOFFERPRICE2 REAL, '
            'ENERGYONLYOFFERMW3 REAL, ENERGYONLYOFFERPRICE3 REAL, '
            'ENERGYONLYOFFERMW4 REAL, ENERGYONLYOFFERPRICE4 REAL, '
            'ENERGYONLYOFFERMW5 REAL, ENERGYONLYOFFERPRICE5 REAL, '
            'ENERGYONLYOFFERMW6 REAL, ENERGYONLYOFFERPRICE6 REAL, '
            'ENERGYONLYOFFERMW7 REAL, ENERGYONLYOFFERPRICE7 REAL, '
            'ENERGYONLYOFFERMW8 REAL, ENERGYONLYOFFERPRICE8 REAL, '
            'ENERGYONLYOFFERMW9 REAL, ENERGYONLYOFFERPRICE9 REAL, '
            'ENERGYONLYOFFERMW10 REAL, ENERGYONLYOFFERPRICE10 REAL, '
            'INSERTEDAT TEXT DEFAULT (datetime(\'now\'))'
            ')'
        )
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS SETTLEMENTPOINTPRICES ('
            'DELIVERYDATE DATE, DELIVERYHOUR INTEGER, DELIVERYINTERVAL INTEGER, SETTLEMENTPOINTNAME TEXT, '
            'SETTLEMENTPOINTTYPE TEXT, AVGSETTLEMENTPOINTPRICE REAL, '
            'DSTFLAG TEXT, INSERTEDAT TEXT'
            ')'
        )

        conn.commit()

    def setup_metadata_tables(self):
        """Setup metadata tables for tracking processing state"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Add this table for caching bundle docIds
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bundle_docids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_type TEXT NOT NULL,
                bundle_date TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                post_datetime TEXT,
                friendly_name TEXT,
                UNIQUE(product_type, bundle_date)
            )
        """)
        conn.commit()
        conn.close()

    async def get_bundle_docid_from_db(self, product_type: str, bundle_date: datetime):
        async with aiosqlite.connect(self.db_path) as conn:
            date_str = bundle_date.strftime('%Y-%m')
            async with conn.execute('''
                SELECT doc_id, post_datetime, friendly_name FROM bundle_docids
                WHERE product_type = ? AND bundle_date = ?
            ''', (product_type, date_str)) as cur:
                row = await cur.fetchone()
            if row:
                return {'docId': row[0], 'postDatetime': row[1], 'friendlyName': row[2]}
            return None

    async def save_bundle_docid_to_db(self, product_type: str, bundle_date: datetime, doc_id: str, post_datetime: str, friendly_name: str):
        async with aiosqlite.connect(self.db_path) as conn:
            date_str = bundle_date.strftime('%Y-%m')
            async with conn.execute('''
                SELECT doc_id FROM bundle_docids WHERE product_type = ? AND bundle_date = ?
            ''', (product_type, date_str)) as cur:
                row = await cur.fetchone()
            if row:
                if row[0] == doc_id:
                    logger.info(
                        f"DocId {doc_id} for {product_type} {date_str} already exists, skipping insert.")
                else:
                    logger.warning(
                        f"Conflicting docId for {product_type} {date_str}: existing {row[0]}, new {doc_id}. Keeping existing.")
                return
            await conn.execute('''
                INSERT INTO bundle_docids (product_type, bundle_date, doc_id, post_datetime, friendly_name)
                VALUES (?, ?, ?, ?, ?)
            ''', (product_type, date_str, doc_id, post_datetime, friendly_name))
            await conn.commit()

    def store_dataframes_batch(self, dataframes: Dict[str, pd.DataFrame]):
        """Store processed dataframes to database using batch inserts"""
        conn = sqlite3.connect(self.db_path)
        try:
            logger.debug("Storing dataframes to database...")
            # Store each dataframe
            table_mapping = {
                'bid_awards': 'DAM_ENERGY_BID_AWARDS',
                'bids': 'DAM_ENERGY_BIDS',
                'offer_awards': 'DAM_ENERGY_OFFER_AWARDS',
                'offers': 'DAM_ENERGY_OFFERS',
                'settlement_prices': 'SETTLEMENTPOINTPRICES'
            }

            for key, table_name in table_mapping.items():
                # skip missing or empty DataFrames
                df = dataframes.get(key)
                if df is None or df.empty:
                    continue
                logger.debug("Processing %s for table %s", key, table_name)
                logger.debug("Duplicates detected: %s", df.duplicated().sum())
                logger.debug("Sample data:\n%s", df.head())
                logger.debug("DataFrame shape: %s", df.shape)
                # Debug statements  to check for content of database

                logger.debug("Unique dates in %s: %s", key, df['DELIVERYDATE'].unique(
                ) if 'DELIVERYDATE' in df.columns else 'N/A')
                logger.debug("Unique settlement points in %s: %s", key, df['SETTLEMENTPOINTNAME'].unique(
                ) if 'SETTLEMENTPOINTNAME' in df.columns else 'N/A')
                logger.debug("Unique QSEs in %s: %s", key, df['QSENAME'].unique(
                ) if 'QSENAME' in df.columns else 'N/A')
                # Ensure DataFrame is not empty
                logger.debug("Unique HOURENDING in %s: %s", key, df['HOURENDING'].unique(
                ) if 'HOURENDING' in df.columns else 'N/A')
                logger.debug("Unique DELIVERYINTERVAL in %s: %s", key, df['DELIVERYINTERVAL'].unique(
                ) if 'DELIVERYINTERVAL' in df.columns else 'N/A')
                # Prepare DataFrame for SQL insert
                try:
                    df = df.copy()
                    # Rename ID and indicator columns to match database schema
                    df.rename(columns={
                        'ENERGYONLYBIDID': 'BIDID',
                        'ENERGYONLYOFFERID': 'OFFERID',
                        'MULTIHOURBLOCKINDICATOR': 'MULTIHOURBLOCK',
                        'BLOCKCURVEINDICATOR': 'BLOCKCURVE'
                    }, inplace=True)
                    # also rename the aggregated price for settlement_prices
                    # ...existing code...
                    if key == 'settlement_prices':
                        logger.debug(
                            "Sample of Settlement Point Prices table: %s", df.head())
                        # If the DataFrame has SETTLEMENTPOINTPRICE, aggregate and rename
                        if 'SETTLEMENTPOINTPRICE' in df.columns:
                            group_cols = ['DELIVERYDATE', 'DELIVERYHOUR',
                                          'SETTLEMENTPOINTNAME']
                            agg_dict = {'SETTLEMENTPOINTPRICE': 'mean'}
                            for col in df.columns:
                                if col not in group_cols + ['SETTLEMENTPOINTPRICE']:
                                    agg_dict[col] = 'first'
                            logger.debug(
                                "Settlement prices aggregation groups: %s", group_cols)
                            logger.debug(
                                "Columns before aggregation: %s", df.columns)
                            df = df.groupby(
                                group_cols, as_index=True).agg(agg_dict).reset_index()
                            if "Index" in df.columns:
                                df.drop(columns=["Index"], inplace=True)
                            # Rename to match DB schema
                            df.rename(
                                columns={'SETTLEMENTPOINTPRICE': 'AVGSETTLEMENTPOINTPRICE'}, inplace=True)
                            logger.debug(
                                "After aggregation, settlement_prices sample:\n%s", df.head())
                    # ...existing code...
                    df['INSERTEDAT'] = datetime.now().isoformat()
                    logger.debug("Unique DELIVERYDATE in %s: %s",
                                 key, df['DELIVERYDATE'].unique())
                    df.to_sql(
                        table_name,
                        conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=min(
                            CHUNK_SIZE, MAX_SQL_BATCH_SIZE // len(df.columns))
                    )
                    logger.info("Stored %d rows to %s", len(df), table_name)
                except sqlite3.IntegrityError as e:
                    logger.warning(
                        "Unique constraint violation on %s, skipping duplicates: %s",
                        table_name, e
                    )
                except Exception as e:
                    logger.error(
                        "Failed to store %s to %s: %s",
                        key, table_name, e
                    )
                    self.update_stats('db_errors')
        finally:
            conn.close()

    def create_final_table_optimized(self, spp_start_date, spp_end_date) -> None:
        """Create final table using pandas DataFrames and to_sql for merging and writing."""
        import pandas as pd
        import sqlite3
        from datetime import datetime

        conn = sqlite3.connect(self.db_path)

        # 2) Read source tables into DataFrames with date filtering
        start_spp = spp_start_date.strftime('%m/%d/%Y')
        end_spp = spp_end_date.strftime('%m/%d/%Y')

        offers = pd.read_sql(
            "SELECT * FROM DAM_ENERGY_OFFERS WHERE DELIVERYDATE BETWEEN ? AND ?",
            conn,
            params=(start_spp, end_spp)
        )
        offer_awards = pd.read_sql(
            "SELECT * FROM DAM_ENERGY_OFFER_AWARDS WHERE DELIVERYDATE BETWEEN ? AND ?",
            conn,
            params=(start_spp, end_spp)
        )
        bids = pd.read_sql(
            "SELECT * FROM DAM_ENERGY_BIDS WHERE DELIVERYDATE BETWEEN ? AND ?",
            conn,
            params=(start_spp, end_spp)
        )
        bid_awards = pd.read_sql(
            "SELECT * FROM DAM_ENERGY_BID_AWARDS WHERE DELIVERYDATE BETWEEN ? AND ?",
            conn,
            params=(start_spp, end_spp)
        )
        spp = pd.read_sql(
            "SELECT * FROM SETTLEMENTPOINTPRICES WHERE DELIVERYDATE BETWEEN ? AND ?",
            conn,
            params=(start_spp, end_spp)
        )

        logger.debug("Loaded data from database:")
        logger.debug("Unique Dates from offers: %s",
                     offers['DELIVERYDATE'].unique())
        logger.debug("Unique Dates from offer_awards: %s",
                     offer_awards['DELIVERYDATE'].unique())
        logger.debug("Unique Dates from bids: %s",
                     bids['DELIVERYDATE'].unique())
        logger.debug("Unique Dates from bid_awards: %s",
                     bid_awards['DELIVERYDATE'].unique())
        logger.debug("Unique Dates from spp: %s", spp['DELIVERYDATE'].unique())

        # --- Deduplicate core tables before merging ---
        # Drop duplicate bids by unique key
        bids = bids.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING",
                    "QSENAME", "SETTLEMENTPOINTNAME", "BIDID"],
            keep="first"
        )
        bid_awards = bid_awards.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING",
                    "QSENAME", "SETTLEMENTPOINTNAME", "BIDID"],
            keep="first"
        )

        # Drop duplicate offers by unique key
        offers = offers.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING", "QSENAME",
                    "SETTLEMENTPOINTNAME", "OFFERID"],
            keep="first"
        )
        offer_awards = offer_awards.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING", "QSENAME",
                    "SETTLEMENTPOINTNAME", "OFFERID"],
            keep="first"
        )

        # Drop duplicate settlement point prices by unique key
        spp = spp.drop_duplicates(
            subset=["DELIVERYDATE", "DELIVERYHOUR",
                    "DELIVERYINTERVAL", "SETTLEMENTPOINTNAME"],
            keep="first"
        )

        logger.debug("Deduplicated DataFrames:")
        logger.debug("Offers unique dates: %s",
                     offers['DELIVERYDATE'].unique())
        logger.debug("Offer Awards unique dates: %s",
                     offer_awards['DELIVERYDATE'].unique())
        logger.debug("Bids unique dates: %s", bids['DELIVERYDATE'].unique())
        logger.debug("Bid Awards unique dates: %s",
                     bid_awards['DELIVERYDATE'].unique())
        logger.debug("Settlement Point Prices unique dates: %s",
                     spp['DELIVERYDATE'].unique())

        # Convert int64 to int32 and float64 to float32 for all intermediate tables
        for name, df in {
            "offers": offers,
            "offer_awards": offer_awards,
            "bids": bids,
            "bid_awards": bid_awards,
            "spp": spp
        }.items():
            # find all int64 and float64 columns
            int_cols = df.select_dtypes(include=["int64"]).columns
            float_cols = df.select_dtypes(include=["float64"]).columns

            # downcast to smaller types
            if not int_cols.empty:
                df[int_cols] = df[int_cols].astype("int32")
            if not float_cols.empty:
                df[float_cols] = df[float_cols].astype("float32")

            logger.debug(
                f"Dtype conversion for {name}: "
                f"{len(int_cols)} int64 to int32, {len(float_cols)} float64 to float32"
            )
        logger.debug("Data types after conversion:")
        logger.debug("Offer uniques dates: %s",
                     offers['DELIVERYDATE'].unique())
        logger.debug("Offer Awards uniques dates: %s",
                     offer_awards['DELIVERYDATE'].unique())
        logger.debug("Bids uniques dates: %s", bids['DELIVERYDATE'].unique())
        logger.debug("Bid Awards uniques dates: %s",
                     bid_awards['DELIVERYDATE'].unique())
        logger.debug("Settlement Point Prices uniques dates: %s",
                     spp['DELIVERYDATE'].unique())

        logger.debug("start_spp & end_spp %s %s", start_spp, end_spp)
        logger.debug("Offers DataFrame shape: %s", offers.shape)
        logger.debug("Offer Awards DataFrame shape: %s", offer_awards.shape)
        logger.debug("Bids DataFrame shape: %s", bids.shape)
        logger.debug("Bid Awards DataFrame shape: %s", bid_awards.shape)
        logger.debug("Settlement Point Prices DataFrame shape: %s", spp.shape)
        # 3) COMBINED_OFFERS: merge offers with offer_awards
        combined_offers = pd.merge(
            offers.drop(columns=["INSERTEDAT"]),
            offer_awards.drop(columns=["INSERTEDAT"]),
            how="right",
            left_on=["DELIVERYDATE", "HOURENDING",
                     "SETTLEMENTPOINTNAME", "QSENAME", "OFFERID"],
            right_on=["DELIVERYDATE", "HOURENDING",
                      "SETTLEMENTPOINTNAME", "QSENAME", "OFFERID"],
        )

        logger.debug("Combined offers unique dates: %s",
                     combined_offers['DELIVERYDATE'].unique())
        combined_offers = combined_offers.rename(columns={
            "ENERGYONLYOFFERAWARDINMW": "OFFER_AWARD_MW",
            "SETTLEMENTPOINTPRICE": "OFFER_AWARD_PRICE"
        })
        combined_offers["INSERTEDAT"] = datetime.now().isoformat()

        combined_offers = combined_offers.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING",
                    "SETTLEMENTPOINTNAME", "QSENAME", "OFFERID"],
            keep="first"
        )

        logger.debug("Combined offers after merge and deduplication: %s",
                     combined_offers['DELIVERYDATE'].unique())
        # Save to DB
        combined_offers.to_sql("COMBINED_OFFERS", conn,
                               if_exists="append", chunksize=min(CHUNK_SIZE, MAX_SQL_BATCH_SIZE // len(combined_offers.columns)), method='multi', index=False)

        logger.debug("combined_offers columns: %s", combined_offers.columns)

        # 4) COMBINED_BIDS: merge bids with bid_awards
        combined_bids = pd.merge(
            bids.drop(columns=["INSERTEDAT"]),
            bid_awards.drop(columns=["INSERTEDAT"]),
            how="right",
            left_on=["DELIVERYDATE", "HOURENDING",
                     "SETTLEMENTPOINTNAME", "QSENAME", "BIDID"],
            right_on=["DELIVERYDATE", "HOURENDING",
                      "SETTLEMENTPOINTNAME", "QSENAME", "BIDID"],
        )
        logger.debug("Combined bids unique dates: %s",
                     combined_bids['DELIVERYDATE'].unique())
        combined_bids = combined_bids.rename(columns={
            "ENERGYONLYBIDAWARDINMW": "BID_AWARD_MW",
            "SETTLEMENTPOINTPRICE": "BID_AWARD_PRICE"
        })
        combined_bids["INSERTEDAT"] = datetime.now().isoformat()
        combined_bids = combined_bids.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING",
                    "SETTLEMENTPOINTNAME", "QSENAME", "BIDID"],
            keep="first"
        )

        logger.debug("Combined bids after merge and deduplication: %s",
                     combined_bids['DELIVERYDATE'].unique())
        # Save to DB
        combined_bids.to_sql("COMBINED_BIDS", conn,
                             if_exists="append", method='multi', chunksize=min(CHUNK_SIZE, MAX_SQL_BATCH_SIZE // len(combined_bids.columns)), index=False)

        logger.debug("combined_bids columns: %s", combined_bids.columns)

        # 5) MERGED_BIDS_OFFERS: inner join on deliverydate, hourending, settlementpointname, qsename
        merged_bids_offers = pd.merge(
            combined_bids.drop(columns=["INSERTEDAT"]),
            combined_offers.drop(columns=["INSERTEDAT"]),
            how="inner",
            on=["DELIVERYDATE", "HOURENDING", "SETTLEMENTPOINTNAME", "QSENAME"],
            suffixes=('_BID', '_OFFER')
        )
        logger.debug("Merged bids and offers unique dates: %s",
                     merged_bids_offers['DELIVERYDATE'].unique())
        merged_bids_offers["INSERTEDAT"] = datetime.now().isoformat()

        merged_bids_offers = merged_bids_offers.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING",
                    "SETTLEMENTPOINTNAME", "QSENAME", "BIDID", "OFFERID"],
            keep="first"
        )
        logger.debug("Merged bids and offers after merge and deduplication: %s",
                     merged_bids_offers['DELIVERYDATE'].unique())
        # Save to DB
        merged_bids_offers.to_sql("MERGED_BIDS_OFFERS", conn,
                                  if_exists="append", method='multi',
                                  chunksize=min(CHUNK_SIZE, MAX_SQL_BATCH_SIZE // len(merged_bids_offers.columns)), index=False)

        logger.debug("merged_bids_offers columns: %s",
                     merged_bids_offers.columns)
        # 6) FINAL: left join with settlement point prices
        final = pd.merge(
            merged_bids_offers.drop(columns=["INSERTEDAT"]),
            spp.drop(columns=["INSERTEDAT"]),
            how="left",
            left_on=["DELIVERYDATE", "HOURENDING", "SETTLEMENTPOINTNAME"],
            right_on=["DELIVERYDATE", "DELIVERYHOUR", "SETTLEMENTPOINTNAME"],
        )
        logger.debug("Final table unique dates: %s",
                     final['DELIVERYDATE'].unique())
        # Select and rename columns for FINAL table
        final_table = final.copy()
        final_table["INSERTEDAT"] = datetime.now().isoformat()

        final_table = final_table.drop_duplicates(
            subset=["DELIVERYDATE", "HOURENDING",
                    "SETTLEMENTPOINTNAME", "QSENAME", "BIDID", "OFFERID"],
            keep="first"
        )
        logger.debug("Final table after merge and deduplication: %s",
                     final_table['DELIVERYDATE'].unique())

        # 7) Write to FINAL table
        final_table.to_sql("FINAL", conn, if_exists="append",
                           chunksize=min(CHUNK_SIZE, MAX_SQL_BATCH_SIZE // len(final_table.columns)), index=False)

        conn.close()
        logger.info(
            "Final table and intermediates created successfully (pandas version)")

    def update_stats(self, stat_name: str, increment: int = 1):
        """Thread-safe stats update"""
        with self.stats_lock:
            self.stats[stat_name] += increment

    def print_stats(self):
        """Print final statistics"""
        with self.stats_lock:
            logger.info("=== Pipeline Statistics ===")
            for key, value in self.stats.items():
                logger.info(f"{key}: {value}")
            logger.info("=========================")
    # ...existing code...

    def get_cache_path(self, cache_type: str, date: datetime, suffix: str = "zip") -> Path:
        date_str = date.strftime("%Y%m%d")
        return self.cache_dir / f"{cache_type}_{date_str}.{suffix}"
# ...existing code...

    def get_cache_path(self, cache_type: str, date: datetime, suffix: str = "json") -> Path:
        date_str = date.strftime("%Y%m%d")
        return self.cache_dir / f"{cache_type}_{date_str}.{suffix}"

    def _get_required_dam_months_for_spp_range(self, spp_start_date: datetime, spp_end_date: datetime) -> Set[str]:
        """Return all DAM months (YYYY-MM) needed for the SPP date range, considering 60-day lag."""
        dam_months = set()
        current = spp_start_date
        while current <= spp_end_date:
            dam_date = current + timedelta(days=60)
            dam_months.add(dam_date.strftime('%Y-%m'))
            current += timedelta(days=1)
        logger.debug(
            "_get_required_dam_months_for_spp_range: dam_months: %s", dam_months)
        return dam_months

    # Main pipeline method - simplified for now, can be extended with async \
    # implementation
    def run_pipeline(self, spp_start_date: datetime, spp_end_date: datetime):
        """Run the complete ETL pipeline with improved processing"""
        import traceback
        start_time = time.time()
        logger.info(
            f"Starting improved ERCOT data pipeline for SPP dates "
            f"{spp_start_date} to {spp_end_date}"
        )
        logger.info(
            f"Tracking {len(self.tracked_qse_names)} QSEs from "
            f"ERCOT_tracking_list.csv"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # (A) and (B): Get all required DAM months for the SPP range
                dam_months = self._get_required_dam_months_for_spp_range(
                    spp_start_date, spp_end_date)
                logger.debug("Required DAM months: %s", dam_months)
                dam_bundles = {}
                for dam_month in dam_months:
                    try:
                        dam_date = datetime.strptime(
                            dam_month + '-01',
                            '%Y-%m-%d'
                        )
                        # logger.debug("dam_bundles %s", dam_bundles)
                        logger.debug("dam_date %s", dam_date)
                        coro = self.fetch_and_extract_dam_bundle(
                            dam_date,
                            temp_dir
                        )
                        if asyncio.iscoroutine(coro):
                            dam_bundles[dam_month] = asyncio.run(
                                coro
                            )
                        else:
                            dam_bundles[dam_month] = coro
                    except Exception:  # pylint: disable=broad-except
                        logger.exception(
                            "Failed to fetch/extract DAM bundle "
                            "for %s",
                            dam_month
                        )
                logger.debug("DAM bundles fetched: %s", dam_bundles.keys())
                # (C): Get all required SPP months for the SPP range
                # build the set of YYYY-MM strings from start to end
                from dateutil.relativedelta import relativedelta
                spp_months = set()
                current_month = spp_start_date.replace(day=1)
                end_month = spp_end_date.replace(
                    day=1) + relativedelta(months=1)
                while current_month <= end_month:
                    spp_months.add(current_month.strftime('%Y-%m'))
                    current_month += relativedelta(months=1)
                logger.debug("Required SPP months: %s", spp_months)
                # (D): Fetch and extract SPP bundles for each month
                # Initialize a dictionary to hold SPP bundles
                spp_bundles = {}
                for spp_month in spp_months:
                    try:
                        spp_date = datetime.strptime(
                            spp_month + '-01',
                            '%Y-%m-%d'
                        )
                        coro = self.fetch_and_extract_spp_bundle(
                            spp_date,
                            temp_dir
                        )
                        if asyncio.iscoroutine(coro):
                            spp_bundles[spp_month] = asyncio.run(
                                coro
                            )
                        else:
                            spp_bundles[spp_month] = coro
                    except Exception:  # pylint: disable=broad-except
                        logger.exception(
                            "Failed to fetch/extract SPP bundle "
                            "for %s",
                            spp_month
                        )
                logger.debug("SPP bundles fetched: %s", spp_bundles.keys())
                # initialize loop date before using it
                current = spp_start_date
                while current <= spp_end_date:
                    spp_months.add(current.strftime('%Y-%m'))
                    current += timedelta(days=1)
                logger.debug("Final SPP months: %s", spp_months)
                current_date = spp_start_date
                total_processed = 0
                while current_date <= spp_end_date + relativedelta(months=1):
                    logger.debug("current_date: %s", current_date)
                    logger.debug("spp_months: %s", spp_months)
                    try:
                        dam_date = current_date + relativedelta(months=2)
                        dam_month = dam_date.strftime('%Y-%m')
                        spp_month = current_date.strftime('%Y-%m')
                        dam_data = dam_bundles.get(dam_month)
                        spp_data = spp_bundles.get(spp_month)
                        logger.debug("dam_date  %s", dam_date)
                        logger.debug("dam_month %s", dam_month)
                        logger.debug("spp_month %s", spp_month)
                        logger.debug("dam_data: %s", dam_data)
                        logger.debug("spp_data: %s", spp_data)
                        # Await extraction if needed
                        if asyncio.iscoroutine(dam_data):
                            dam_data = asyncio.run(dam_data)
                        if asyncio.iscoroutine(spp_data):
                            spp_data = asyncio.run(spp_data)
                        # (C) Merge Bids with Bid Awards, Offers with Offer Awards
                        bid_awards = dam_data.get(
                            '60d_DAM_EnergyBidAwards', pd.DataFrame())
                        bids = dam_data.get(
                            '60d_DAM_EnergyBids', pd.DataFrame())
                        offer_awards = dam_data.get(
                            '60d_DAM_EnergyOnlyOfferAwards', pd.DataFrame())
                        offers = dam_data.get(
                            '60d_DAM_EnergyOnlyOffers', pd.DataFrame())

                        # Fix any misspelled or inconsistent settlement point column names
                        for df in (bids, bid_awards, offers, offer_awards):
                            df.rename(columns={
                                'SETTLTMENTPOINT': 'SETTLEMENTPOINTNAME',
                                'SETTLEMENTPOINT': 'SETTLEMENTPOINTNAME'
                            }, inplace=True)
                        # Merge Bids + Bid Awards
                        # Normalize columns to ensure consistency
                        bids.columns = normalize_headers(bids.columns)
                        bid_awards.columns = normalize_headers(
                            bid_awards.columns)
                        # (E) Filter SPPs by settlement points in awards
                        used_settlement_points = set()
                        if not bid_awards.empty:
                            used_settlement_points.update(
                                bid_awards['SETTLEMENTPOINTNAME'].unique())
                        if not offer_awards.empty:
                            used_settlement_points.update(
                                offer_awards['SETTLEMENTPOINTNAME'].unique())
                        # (D) Filter SPP data
                        spp_df = pd.concat(spp_data, ignore_index=True) if isinstance(
                            spp_data, list) else spp_data
                        if not spp_df.empty and used_settlement_points:
                            spp_df = spp_df[spp_df['SETTLEMENTPOINTNAME'].isin(
                                used_settlement_points)]
                        logger.debug("Unique settlement points in SPP data: %s",
                                     spp_df['SETTLEMENTPOINTNAME'].unique())
                        # Ensure DataFrame columns match the database schema
                        for df in (bid_awards, bids, offer_awards, offers, spp_df):
                            if 'SETTLEMENTPOINT' in df.columns:
                                df.rename(
                                    columns={
                                        'SETTLEMENTPOINT': 'SETTLEMENTPOINTNAME'},
                                    inplace=True
                                )
                        logger.debug("Storing data to .db")
                        # Store to DB (reuse store_dataframes_batch)
                        self.store_dataframes_batch({
                            'bid_awards': bid_awards,
                            'bids': bids,
                            'offer_awards': offer_awards,
                            'offers': offers,
                            'settlement_prices': spp_df
                        })

                        total_processed += 1
                        logger.info(
                            f"Processed SPP date {current_date.date()} (DAM month {dam_month})")
                    except Exception as e:
                        logger.error(
                            f"Error processing SPP date {current_date.date()}: {e}\n{traceback.format_exc()}")
                    logger.debug(
                        f"Processed {total_processed} date pairs so far")
                    # Increment to next day
                    logger.debug("current_date -> current_date + 1 Month" + str(
                        current_date) + " -> " + str(current_date + relativedelta(months=1)))
                    current_date += relativedelta(months=1)
                    # Create final table
                logger.info("Creating optimized final table...")
                self.create_final_table_optimized(
                    current_date.replace(day=1, month=1), current_date.replace(day=31, month=12))
                execution_time = time.time() - start_time
                logger.info(
                    f"Pipeline completed in {execution_time:.2f} seconds")
                logger.info(f"Processed {total_processed} date pairs")
                self.print_stats()
            except Exception as e:
                logger.error(f"Pipeline failed: {e}\n{traceback.format_exc()}")
                raise

    def download_dam_zip_openapi(self, dam_date: datetime, temp_dir: str) -> Path:
        """Download DAM ZIP using ERCOT OpenAPI client, supporting both bundles and legacy archives"""
        dam_emil_id = dam_date.strftime('%Y%m%d')
        dam_zip_path = Path(temp_dir) / f"{dam_emil_id}_dam.zip"
        dam_zip_cache = self.get_cache_path("dam_zip", dam_date, "zip")
        if self.enable_cache and dam_zip_cache.exists():
            import shutil
            shutil.copy(dam_zip_cache, dam_zip_path)
            logger.info(f"Using cached DAM ZIP: {dam_zip_cache}")
            return dam_zip_path

        try:
            # Try bundle first using the PRODUCT_ID
            product_id = ERCOT_PRODUCT_IDS['DAM']['BUNDLE']
            metadata = self.ercot_api.get_bundle_metadata(product_id)
            logger.debug(f"Fetched DAM bundles metadata: {metadata}")

            year_month = dam_date.strftime('%Y-%m')
            bundle = None
            if metadata and metadata.get('bundles'):
                for b in metadata['bundles']:
                    fname = b.get('friendlyName', '')
                    if fname.endswith(year_month) or year_month in fname:
                        bundle = b
                        break
                if not bundle:
                    # no exact match, fall back to first bundle
                    bundle = metadata['bundles'][0]
                    logger.warning(
                        f"No DAM bundle with friendlyName matching '{year_month}'. "
                        f"Using first available: {bundle.get('friendlyName')}"
                    )
                doc_id = bundle['docId']
                self.ercot_api.download_archive(
                    product_id,
                    str(dam_zip_path),
                    download_id=doc_id,
                    is_bundle=True
                )
                logger.info(
                    f"Downloaded DAM ZIP via OpenAPI bundle: {dam_zip_path} "
                    f"(product={product_id}, docId={doc_id}, friendlyName={bundle.get('friendlyName')})"
                )
            else:
                # fallback to legacy archive by EMIL id
                self.ercot_api.download_archive(dam_emil_id, str(dam_zip_path))
                logger.info(
                    f"Downloaded DAM ZIP via legacy archive: {dam_zip_path}"
                )

            if self.enable_cache:
                import shutil
                shutil.copy(dam_zip_path, dam_zip_cache)
            return dam_zip_path

        except FileNotFoundError as e:
            logger.error(str(e))
            raise
        except Exception as e:
            logger.error(f"OpenAPI DAM ZIP download failed: {e}")
            raise

    def get_dam_metadata(self, dam_date: datetime):
        dam_emil_id = dam_date.strftime('%Y%m%d')
        try:
            return self.ercot_api.get_archive_metadata(dam_emil_id)
        except Exception as e:
            logger.error(f"Failed to get DAM metadata: {e}")
            return None

    async def get_bundle_metadata(self, bundle_date: datetime, product_type: str = 'SPP'):
        # Use product ID from ERCOT_PRODUCT_IDS
        product_id = ERCOT_PRODUCT_IDS[product_type]['BUNDLE']
        logger.debug(
            f"Fetching bundle metadata for product_id={product_id}, bundle_date={bundle_date}")
        try:
            metadata = self.ercot_api.get_bundle_metadata(product_id)
            logger.debug(
                f"Bundle metadata response for product_id={product_id}: {metadata}")
            if not metadata or 'bundles' not in metadata or not metadata['bundles']:
                logger.error(f"No bundles found for product_id {product_id}")
                return None
            # Find bundle for the given year and month (ignore day)
            year_month = bundle_date.strftime('%Y-%m')
            logger.debug(
                f"Looking for bundle with postDatetime starting with {year_month}")
            for bundle in metadata['bundles']:
                post_dt = bundle.get('postDatetime', '')
                logger.debug(
                    f"Checking bundle: docId={bundle.get('docId')}, postDatetime={post_dt}, friendlyName={bundle.get('friendlyName')}")
                if post_dt.startswith(year_month):
                    # Save to DB for caching
                    await self.save_bundle_docid_to_db(
                        product_type, bundle_date, bundle['docId'], post_dt, bundle.get('friendlyName', ''))
                    logger.info(
                        f"Found bundle for {year_month}: docId={bundle['docId']}, friendlyName={bundle.get('friendlyName', '')}")
                    return bundle
            logger.error(
                f"No bundle found for {year_month} in product_id {product_id}")
            logger.debug(
                f"Available bundle postDatetimes: {[b.get('postDatetime') for b in metadata['bundles']]}")
            return None
        except Exception as e:
            logger.error(f"Failed to get bundle metadata: {e}")
            return None

    async def download_bundle_openapi(self, bundle_date: datetime, temp_dir: str, product_type: str = 'SPP') -> Path:
        # Check DB for docId first
        bundle_info = await self.get_bundle_docid_from_db(product_type, bundle_date)
        logger.debug("bundle_info: %s", bundle_info)
        if not bundle_info:
            # Fetch metadata and save docid to DB
            bundle_info = await self.get_bundle_metadata(bundle_date, product_type)
            logger.debug("bundle_info: %s", bundle_info)
            await self.save_bundle_docid_to_db(
                product_type, bundle_date,
                bundle_info['docId'],
                bundle_info['postDatetime'],
                bundle_info.get('friendlyName', '')
            )
        doc_id = bundle_info['docId']
        friendly_name = bundle_info.get('friendlyName', '')
        product_id = ERCOT_PRODUCT_IDS[product_type]['BUNDLE']
        bundle_zip_path = Path(
            temp_dir) / f"{bundle_date.strftime('%Y%m%d')}_{product_type.lower()}_bundle.zip"
        logger.debug("Bundle Info: Doc Id %s Friendly Name: %s Product Id: %s bundle_zip_path: %s",
                     doc_id, friendly_name, product_id, bundle_zip_path)
        # Only download if not already present
        if not bundle_zip_path.exists():
            await self.api_client.download_bundle(
                product_id, doc_id, str(bundle_zip_path))
            logger.info(
                f"Downloaded bundle ZIP via OpenAPI: {bundle_zip_path} (docId={doc_id}, {friendly_name})")
        else:
            logger.info(f"Bundle ZIP already exists: {bundle_zip_path}")
        return bundle_zip_path

    async def fetch_and_extract_dam_bundle(self, dam_date: datetime, temp_dir: str) -> dict:
        month_key = dam_date.strftime('%Y-%m')
        bundle_info = await self.get_bundle_docid_from_db('DAM', dam_date)
        if bundle_info:
            logger.info(
                "DAM bundle for %s already processed (docId=%s), skipping re-download.",
                month_key,
                bundle_info['docId'],
            )
            return {}
        elif not bundle_info:
            bundle_info = await self.get_bundle_metadata(dam_date, 'DAM')
            if not bundle_info:
                logger.error(
                    f"Failed to fetch DAM bundle metadata for {month_key}")
                return {}

        # Download the ZIP file synchronously since this helper is not async
        dam_zip = self.download_dam_zip_openapi(dam_date, temp_dir)
        logger.debug("dam_zip filename: %s",  dam_zip.name)
        dam_files: Dict[str, List[pd.DataFrame]] = {
            "60d_DAM_EnergyBids": [],
            "60d_DAM_EnergyBidAwards": [],
            "60d_DAM_EnergyOnlyOffers": [],
            "60d_DAM_EnergyOnlyOfferAwards": [],
        }
        extracted_files: List[str] = []
        file_map: Dict[str, List[str]] = {
            key: [] for key in dam_files.keys()
        }

        def extract_zip_recursive(zip_bytes):
            with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as z:
                for entry in z.namelist():
                    logger.debug("Entry: %s", entry)
                    if entry.lower().endswith('.zip'):
                        logger.debug("Found nested ZIP: %s", entry)
                        # Recursively extract nested ZIPs
                        with z.open(entry) as ef:
                            extract_zip_recursive(ef.read())
                    elif entry.lower().endswith('.csv'):
                        for key in dam_files.keys():
                            if key in entry:
                                with z.open(entry) as cf:
                                    df = pd.read_csv(cf)
                                    df.columns = normalize_headers(df.columns)
                                    dam_files[key].append(df)
                                    extracted_files.append(entry)
                                    file_map[key].append(entry)
                                    logger.debug(
                                        f"Appended {entry} to {key}, shape={df.shape}")

        try:
            with open(dam_zip, 'rb') as f:
                extract_zip_recursive(f.read())
            # Concatenate all DataFrames for each key
            dam_files_final = {}
            for key, dfs in dam_files.items():
                if dfs:
                    dam_files_final[key] = pd.concat(dfs, ignore_index=True)
                    logger.debug(
                        f"fetch_and_extract_dam_bundle: All files for {key}: {file_map[key]}")
                    logger.info(
                        f"fetch_and_extract_dam_bundle: {key} total rows: {dam_files_final[key].shape[0]}")
                else:
                    logger.warning(
                        f"fetch_and_extract_dam_bundle: No files found for {key}")
            logger.debug(
                f"fetch_and_extract_dam_bundle: All DAM extracted file names: {extracted_files}")
            logger.info(
                f"fetch_and_extract_dam_bundle: Total DAM CSVs processed: {sum(len(dfs) for dfs in dam_files.values())}")
            return dam_files_final
        except Exception as e:
            logger.error(f"Failed to fetch/extract DAM bundle: {e}")
            return {}

    async def fetch_and_extract_spp_bundle(self, spp_date: datetime, temp_dir: str) -> list:
        product_type = 'SPP'
        month_key = spp_date.strftime('%Y-%m')
        bundle_info = await self.get_bundle_docid_from_db(product_type, spp_date)
        if bundle_info:
            logger.info(
                f"SPP bundle for {month_key} already processed (docId={bundle_info['docId']}). Skipping re-download.")
            return []
        product_id = ERCOT_PRODUCT_IDS[product_type]['BUNDLE']
        bundles_processed = 0
        frames: List[pd.DataFrame] = []
        extracted_files = []

        def extract_zip_recursive(zip_bytes):
            with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as z:
                for entry in z.namelist():
                    if entry.lower().endswith('.zip'):
                        with z.open(entry) as ef:
                            extract_zip_recursive(ef.read())
                    elif entry.lower().endswith('.csv'):
                        with z.open(entry) as cf:
                            df = pd.read_csv(cf)
                            df.columns = normalize_headers(df.columns)
                            frames.append(df)
                            extracted_files.append(entry)

        try:
            # Fetch all bundles for the month/year of spp_date
            metadata = self.ercot_api.get_bundle_metadata(product_id)
            if not metadata or 'bundles' not in metadata or not metadata['bundles']:
                logger.error(
                    f"No SPP bundles found for product_id {product_id}")
                return frames
            year_month = spp_date.strftime('%Y-%m')
            bundles = [b for b in metadata['bundles'] if b.get(
                'postDatetime', '').startswith(year_month)]
            if not bundles:
                logger.error(
                    f"No SPP bundles found for {year_month} in product_id {product_id}")
                return frames
            logger.debug("Starting to process SPP bundles...")
            logger.debug("SPP bundles to process: %s", bundles)
            # ...existing code in fetch_and_extract_spp_bundle...

            for bundle in bundles:
                doc_id = bundle['docId']
                friendly_name = bundle.get('friendlyName', '')
                bundle_zip_path = Path(temp_dir) / f"{doc_id}_spp_bundle.zip"
                spp_zip_cache = self.get_cache_path("spp_zip", spp_date, "zip")

                # --- Caching logic start ---
                if self.enable_cache and spp_zip_cache.exists():
                    import shutil
                    shutil.copy(spp_zip_cache, bundle_zip_path)
                    logger.info(f"Using cached SPP ZIP: {spp_zip_cache}")
                elif not bundle_zip_path.exists():
                    self.ercot_api.download_bundle(
                        product_id, doc_id, str(bundle_zip_path))
                    logger.info(
                        f"Downloaded SPP bundle ZIP: {bundle_zip_path} (docId={doc_id}, {friendly_name})")
                    # Save to cache after download
                    if self.enable_cache:
                        import shutil
                        shutil.copy(bundle_zip_path, spp_zip_cache)
                else:
                    logger.info(
                        f"SPP bundle ZIP already exists: {bundle_zip_path}")
                # --- Caching logic end ---

                # Recursively extract all CSVs from this bundle
                try:
                    with open(bundle_zip_path, 'rb') as f:
                        extract_zip_recursive(f.read())
                    bundles_processed += 1
                    logger.info(
                        f"Extracted {len(frames)} SPP CSV files from bundle {doc_id} ({friendly_name})")
                except Exception as e:
                    logger.error(f"Failed to extract SPP bundle {doc_id}: {e}")
            # ...existing code...
            logger.info(
                f"Total SPP bundles processed for {year_month}: {bundles_processed}")
            logger.debug(f"All SPP extracted file names: {extracted_files}")
        except Exception as e:
            logger.error(f"Failed to fetch/extract SPP bundles: {e}")
        return frames

    def update(self, spp_start_date, spp_end_date, dam_start_date, dam_end_date):
        """
        Update the pipeline by filtering and appending new data for the given date ranges.
        - Loads DAM and SPP data from ERCOT API endpoints.
        - Filters DAM tables (offers, bids, offer_awards, bid_awards) by tracked QSEs.
        - Filters settlement points based on active awards for those QSEs.
        - Aggregates SPP data by DELIVERYDATE, DELIVERHOUR, SETTLEMENTPOINTNAME, averaging SETTLEMENTPOINTPRICE over DELIVERYINTERVAL, and taking the first of INSERTEDAT, DSTGLAD, SETTLEMENTPOINTTYPE.
        - Appends filtered/aggregated data to the existing tables.
        """
        import pandas as pd

        # 1. Get tracked QSEs
        tracked_qses = set(self.tracked_qse_names)
        if not tracked_qses:
            tracking_csv = None
            for path in ["_data/ERCOT_tracking_list.csv", "ercot_scraping/_data/ERCOT_tracking_list.csv"]:
                try:
                    tracking_csv = pd.read_csv(path)
                    break
                except Exception:
                    continue
            if tracking_csv is not None:
                tracked_qses = set(
                    tracking_csv[tracking_csv.columns[0]].dropna().unique())
            else:
                raise RuntimeError("Could not load tracked QSEs from CSV.")

        # 2. Load DAM data from API and filter by tracked QSEs
        dam_offers = self.current_60_dam_energy_only_offers(
            dam_start_date, dam_end_date)
        dam_bids = self.current_60_dam_energy_only_bids(
            dam_start_date, dam_end_date)
        dam_offer_awards = self.current_60_dam_energy_only_offer_awards(
            dam_start_date, dam_end_date)
        dam_bid_awards = self.current_60_dam_energy_only_bid_awards(
            dam_start_date, dam_end_date)

        dam_offers = dam_offers[dam_offers["QSENAME"].isin(tracked_qses)]
        dam_bids = dam_bids[dam_bids["QSENAME"].isin(tracked_qses)]
        dam_offer_awards = dam_offer_awards[dam_offer_awards["QSENAME"].isin(
            tracked_qses)]
        dam_bid_awards = dam_bid_awards[dam_bid_awards["QSENAME"].isin(
            tracked_qses)]

        # 3. Find settlement points with active awards for tracked QSEs
        active_offer_points = set(
            dam_offer_awards.loc[dam_offer_awards["ENERGYONLYOFFERAWARDINMW"] > 0, "SETTLEMENTPOINTNAME"])
        active_bid_points = set(
            dam_bid_awards.loc[dam_bid_awards["ENERGYONLYBIDAWARDINMW"] > 0, "SETTLEMENTPOINTNAME"])
        active_points = active_offer_points.union(active_bid_points)

        # 4. Load and aggregate SPP data from API, filter by active points
        spp_prices = self.current_spp_node_zone_hub(
            spp_start_date, spp_end_date)
        spp_prices = spp_prices[spp_prices["SETTLEMENTPOINTNAME"].isin(
            active_points)]

        if not spp_prices.empty:
            # aggregate settlement point prices by date/hour/point
            agg_dict = {
                "SETTLEMENTPOINTPRICE": "mean",
                "DELIVERYINTERVAL": "first",
                "INSERTEDAT": "first",
                "DSTFLAG": "first",
                "SETTLEMENTPOINTTYPE": "first"
            }
            group_cols = ["DELIVERYDATE",
                          "DELIVERYHOUR", "SETTLEMENTPOINTNAME"]
            spp_prices = (
                spp_prices
                .groupby(group_cols, as_index=False)
                .agg(agg_dict)
                .rename(columns={"SETTLEMENTPOINTPRICE": "AVGSETTLEMENTPOINTPRICE"})
            )

        # 5. Append filtered / aggregated data to existing tables
        self.store_dataframes_batch({
            "bid_awards": dam_bid_awards,
            "bids":      dam_bids,
            "offer_awards": dam_offer_awards,
            "offers":    dam_offers,
            "settlement_prices": spp_prices
        })

        # 6. Append to FINAL table
        try:
            logger.info(
                f"Appending new FINAL records for SPP dates {spp_start_date} to {spp_end_date}"
            )
            self.create_final_table_optimized(spp_start_date, spp_end_date)
            logger.info("Successfully appended to FINAL table")
        except Exception as e:
            logger.error(f"Failed to append to FINAL table: {e}")

    def fetch_current_api_data(self, url: str, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetches all paginated data from the ERCOT API endpoint for the given date range.
        Returns a list of records (dicts).
        """
        records = []
        page = 1
        while True:
            params = {
                "deliveryDateFrom": start_date,
                "deliveryDateTo": end_date,
                "page": page
            }
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            result = resp.json()
            # --- replace $SELECTION_PLACEHOLDER$ with this block ---
            raw = result.get("data", [])
            # flatten dict-of-lists or dict-of-dicts into one list
            candidates = []
            if isinstance(raw, dict):
                for v in raw.values():
                    if isinstance(v, list):
                        candidates.extend(v)
                    elif isinstance(v, dict):
                        candidates.append(v)
            elif isinstance(raw, list):
                candidates = raw
            # finally, only keep dictionaries
            for item in candidates:
                if isinstance(item, dict):
                    records.append(item)
            records.extend(data)
            meta = result.get("_meta", {})
            total_pages = meta.get("totalPages", 1)
            if page >= total_pages:
                break
            page += 1
        return records

    def current_60_dam_energy_only_offer_awards(self, start_date: str, end_date: str) -> pd.DataFrame:
        url = "https://api.ercot.com/api/public-reports/np3-966-er/60_dam_energy_only_offer_awards"
        data = self.fetch_current_api_data(url, start_date, end_date)
        return pd.DataFrame(data)

    def current_60_dam_energy_only_offers(self, start_date: str, end_date: str) -> pd.DataFrame:
        url = "https://api.ercot.com/api/public-reports/np3-966-er/60_dam_energy_only_offers"
        data = self.fetch_current_api_data(url, start_date, end_date)
        return pd.DataFrame(data)

    def current_60_dam_energy_only_bids(self, start_date: str, end_date: str) -> pd.DataFrame:
        url = "https://api.ercot.com/api/public-reports/np3-966-er/60_dam_energy_only_bids"
        data = self.fetch_current_api_data(url, start_date, end_date)
        return pd.DataFrame(data)

    def current_60_dam_energy_only_bid_awards(self, start_date: str, end_date: str) -> pd.DataFrame:
        url = "https://api.ercot.com/api/public-reports/np3-966-er/60_dam_energy_only_bid_awards"
        data = self.fetch_current_api_data(url, start_date, end_date)
        return pd.DataFrame(data)

    def current_spp_node_zone_hub(self, start_date: str, end_date: str) -> pd.DataFrame:
        url = "https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub"
        data = self.fetch_current_api_data(url, start_date, end_date)
        return pd.DataFrame(data)
# --- ASYNC ERCOT DATA PROCESSOR INTEGRATION ---

# --- ENTRY POINT ---


def parse_flexible_date(date_str: str, is_start: bool = True) -> datetime:
    """Parse a date string as either %Y-%m-%d or %Y-%m. If %Y-%m, use first/last day."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            dt = datetime.strptime(date_str, "%Y-%m")
            if is_start:
                return dt.replace(day=1)
            else:
                last_day = calendar.monthrange(dt.year, dt.month)[1]
                return dt.replace(day=last_day)
        except ValueError:
            raise ValueError(
                f"Date '{date_str}' must be in YYYY-MM-DD or YYYY-MM format."
            )


if __name__ == "__main__":
    import sys
    import argparse
    from pathlib import Path
    import shutil
    parser = argparse.ArgumentParser(description="ERCOT Data Pipeline")
    parser.add_argument('--start', type=str, required=True,
                        help='Start date (YYYY-MM-DD or YYYY-MM)')
    parser.add_argument('--end', type=str, required=True,
                        help='End date (YYYY-MM-DD or YYYY-MM)')
    parser.add_argument(
        '--db', type=str, default='ercot_data_improved.db', help='SQLite DB path')
    parser.add_argument('--mode', type=str, choices=['sync', 'async', 'bundle-download'],
                        default='sync', help='Pipeline mode: sync (default), async, or bundle-download.')
    parser.add_argument('--download-workers', type=int, default=3,
                        help='Number of download workers (default: 3)')
    parser.add_argument('--processing-workers', type=int, default=6,
                        help='Number of processing workers (default: 6)')
    parser.add_argument('--storage-workers', type=int, default=4,
                        help='Number of storage workers (default: 4)')
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Remove the cache directory before running the pipeline"
    )
    parser.add_argument("--enable-cache",
                        action="store_true",
                        default=True,
                        help="Enable caching of downloaded files (default: True)"
                        )

    parser.add_argument("--clear-db",
                        action="store_true",
                        help="Clear the database before running the pipeline"
                        )

    args = parser.parse_args()

    if args.clear_cache:

        cache_dir = Path("_cache")
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            print(f"Cache cleared at {cache_dir}")
        else:
            print(f"No cache directory to clear at {cache_dir}")

    if args.clear_db:
        # Remove the main DB and any year‐specific DBs (e.g. mydb_2021.db, mydb_2022.db)
        base = args.db.rstrip('.db')
        pattern = f"{base}*.db"
        removed = []
        for db_file in Path('.').glob(pattern):
            try:
                db_file.unlink()
                removed.append(str(db_file))
            except Exception as e:
                print(f"Failed to remove {db_file}: {e}")
        if removed:
            print(f"Database files cleared: {', '.join(removed)}")
        else:
            print(f"No database files matched pattern '{pattern}'")
            # exit so we don't run the pipeline

    start_date = parse_flexible_date(args.start, is_start=True)
    end_date = parse_flexible_date(args.end, is_start=False)

    # Ensure only one pipeline is run at a time
    if args.mode == 'sync':
        # Split multi‐year range into per‐year runs with separate DBs
        current_start = start_date
        while current_start <= end_date:
            # end of calendar year or overall end date
            year_end = datetime(current_start.year, 12, 31)
            period_end = min(year_end, end_date)
            # construct a year‐specific database filename
            base_db = args.db.rstrip('.db')
            year_db = f"{base_db}_{current_start.year}.db"
            logger.debug("Running pipeline for year DB: %s", year_db)
            logger.debug("Current start date: %s, Period end date: %s",
                         current_start, period_end)
            pipeline = ImprovedERCOTDataPipeline(
                db_path=year_db, enable_cache=args.enable_cache)
            pipeline.run_pipeline(
                current_start, period_end)  # +9 weeks to cover all SPP data
            # move to next day after this period
            current_start = period_end + timedelta(days=1)
    elif args.mode == "update":
        # Initialize pipeline and run the update operation against the existing DB
        try:
            pipeline = ImprovedERCOTDataPipeline(
                db_path=args.db, enable_cache=args.enable_cache)
            # Read from the database to get the last DeliveryDate for the SPP table
            db_file = args.db
            db_conn = sqlite3.connect(db_file)
            latest_spp_date = pd.read_sql(
                "SELECT MAX(DELIVERYDATE) FROM spp_settlement_prices",
                db_conn
            ).values[0][0]
            latest_dam_date = pd.read_sql(
                "SELECT MAX(DELIVERYDATE) FROM dam_energy_offers",
                db_conn
            ).values[0][0]
            end_spp_date = datetime.now() - timedelta(days=60)
            end_dam_date = datetime.now()
            db_conn.close()
            pipeline.update(spp_start_date=latest_spp_date,
                            spp_end_date=end_spp_date,
                            dam_start_date=latest_dam_date,
                            dam_end_date=end_dam_date)
        except sqlite3.OperationalError as e:
            print(f"Database error: {e}")
            print("Ensure the database exists and is accessible.")
            sys.exit(1)
    else:
        print("ERROR: Invalid mode or multiple pipeline modes selected. Only one pipeline can be run at a time.")
        sys.exit(1)
