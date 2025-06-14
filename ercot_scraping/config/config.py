import os
import logging

import requests

from dotenv import load_dotenv
from ercot_scraping.config.queries import *  # Import only necessary queries
from ercot_scraping.config.column_mappings import *
from ercot_scraping.database.data_models import BidAward, Bid, Offer, OfferAward, SettlementPointPrice

# Load environment variables from .env file
load_dotenv()

# ERCOT API Secrets
ERCOT_USERNAME = os.getenv("ERCOT_API_USERNAME")
ERCOT_PASSWORD = os.getenv("ERCOT_API_PASSWORD")
ERCOT_API_SUBSCRIPTION_KEY = os.getenv("ERCOT_API_SUBSCRIPTION_KEY")
# ERCOT API URLs and headers
ERCOT_API_BASE_URL_DAM = r"https://api.ercot.com/api/public-reports/np3-966-er"
ERCOT_API_BASE_URL_SETTLEMENT = r"https://api.ercot.com/api/public-reports/np6-905-cd"
# Get the Authorization response
AUTH_URL = f"https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token?username={ERCOT_USERNAME}&password={ERCOT_PASSWORD}&grant_type=password&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70&response_type=id_token"

# Sign In/Authenticate
AUTH_RESPONSE = requests.post(AUTH_URL, timeout=3600)
# Only need the id_token for use in follow up requests headers.
ERCOT_ID_TOKEN = AUTH_RESPONSE.json().get("id_token")
ERCOT_API_REQUEST_HEADERS = {
    "Ocp-Apim-Subscription-Key": ERCOT_API_SUBSCRIPTION_KEY,
    "Authorization": f"Bearer {ERCOT_ID_TOKEN}",
}
# Database path
ERCOT_DB_NAME = "_data/ercot_data.db"
# CSV file path
QSE_FILTER_CSV = "_data/ERCOT_tracking_list.csv"

# Archive API endpoints
ERCOT_ARCHIVE_API_BASE_URL = "https://api.ercot.com/api/public-reports/archive"
ERCOT_ARCHIVE_PRODUCT_IDS = {
    "SPP": "NP6-905-CD",  # Settlement Point Prices
    "DAM": {
        "BIDS": "NP3-966-er",  # DAM Energy Bids
        "BID_AWARDS": "NP3-966-ER",  # DAM Energy Bid Awards
        "OFFERS": "NP3-966-ER",  # DAM Energy Offers
        "OFFER_AWARDS": "NP3-966-ER",  # DAM Energy Offer Awards
    }
}
# File names for the DAM archive data
DAM_FILENAMES = [
    "60d_DAM_EnergyBidAwards-",
    "60d_DAM_EnergyBids-",
    "60d_DAM_EnergyOnlyOfferAwards-",
    "60d_DAM_EnergyOnlyOffers-",
]

FILE_LIMITS = {
    "DAM": 25,
    "SPP": 1000,  # SPP archive files limit
}

# API Rate Limiting and Request Settings
API_RATE_LIMIT_REQUESTS = 10
API_RATE_LIMIT_INTERVAL = 60
DEFAULT_BATCH_DAYS = 7
MAX_DATE_RANGE = 100  # example value; tests assume a high cap
# New flag to disable sleep during tests to reduce runtime
DISABLE_RATE_LIMIT_SLEEP = False
API_MAX_ARCHIVE_FILES = 1000
API_CUTOFF_DATE = "2023-12-11"
REQUEST_TIMEOUT = 30
API_MAX_DAM_BATCH_SIZE = 25  # Maximum allowed by ERCOT DAM archive API

# DAM switches to archive before this date
# Setting this to large date since the Archive API is faster than the current API for the DAM data.
DAM_ARCHIVE_CUTOFF_DATE = "2099-02-01"
# SPP switches to archive before this date
SPP_ARCHIVE_CUTOFF_DATE = "2099-12-11"

DAM_TABLE_DATA_MAPPING = {
    "BID_AWARDS": {
        "model_class": BidAward,
        "insert_query": BID_AWARDS_INSERT_QUERY
    },
    "BIDS": {
        "model_class": Bid,
        "insert_query": BIDS_INSERT_QUERY
    },
    "OFFER_AWARDS": {
        "model_class": OfferAward,
        "insert_query": OFFER_AWARDS_INSERT_QUERY
    },
    "OFFERS": {
        "model_class": Offer,
        "insert_query": OFFERS_INSERT_QUERY
    },
    "SETTLEMENT_POINT_PRICES": {
        "model_class": SettlementPointPrice,
        "insert_query": SETTLEMENT_POINT_PRICES_INSERT_QUERY
    }
}

# Configure logging
LOGGER = logging.getLogger(__name__)

# Ensure environment variables are loaded correctly
LOGGER.info("ERCOT_API_BASE_URL_DAM: %s", ERCOT_API_BASE_URL_DAM)
LOGGER.info("ERCOT_API_BASE_URL_SETTLEMENT: %s", ERCOT_API_BASE_URL_SETTLEMENT)
