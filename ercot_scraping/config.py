import os
import requests
import logging

from dotenv import load_dotenv

from ercot_scraping.data_models import BidAward, Bid, Offer, OfferAward

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
AUTH_RESPONSE = requests.post(AUTH_URL)
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
    "DAM_BIDS": "NP3-966-er",  # DAM Energy Bids
    "DAM_BID_AWARDS": "NP3-966-ER",  # DAM Energy Bid Awards
    "DAM_OFFERS": "NP3-966-ER",  # DAM Energy Offers
    "DAM_OFFER_AWARDS": "NP3-966-ER",  # DAM Energy Offer Awards
}
# File names for the DAM archive data
DAM_FILENAMES = [
    "60d_DAM_EnergyBidAwards-",
    "60d_DAM_EnergyBids-",
    "60d_DAM_EnergyOnlyOfferAwards-",
    "60d_DAM_EnergyOnlyOffers-",
]


# API Rate Limiting
API_RATE_LIMIT_REQUESTS = 30  # requests per minute
API_RATE_LIMIT_INTERVAL = 60  # seconds
API_MAX_ARCHIVE_FILES = 1000  # maximum files per archive request
API_CUTOFF_DATE = "2023-12-11"  # date when archive API becomes necessary

# SQL Queries
SETTLEMENT_POINT_PRICES_INSERT_QUERY = """
    INSERT INTO SETTLEMENT_POINT_PRICES (DeliveryDate, DeliveryHour, DeliveryInterval,
                                         SettlementPointName, SettlementPointType,
                                         SettlementPointPrice, DSTFlag)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
BID_AWARDS_INSERT_QUERY = """
    INSERT INTO BID_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                            EnergyOnlyBidAwardMW, SettlementPointPrice, BidId)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
BIDS_INSERT_QUERY = """
    INSERT INTO BIDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                      EnergyOnlyBidMW1, EnergyOnlyBidPrice1, EnergyOnlyBidMW2, EnergyOnlyBidPrice2,
                      EnergyOnlyBidMW3, EnergyOnlyBidPrice3, EnergyOnlyBidMW4, EnergyOnlyBidPrice4,
                      EnergyOnlyBidMW5, EnergyOnlyBidPrice5, EnergyOnlyBidMW6, EnergyOnlyBidPrice6,
                      EnergyOnlyBidMW7, EnergyOnlyBidPrice7, EnergyOnlyBidMW8, EnergyOnlyBidPrice8,
                      EnergyOnlyBidMW9, EnergyOnlyBidPrice9, EnergyOnlyBidMW10, EnergyOnlyBidPrice10,
                      EnergyOnlyBidID, MultiHourBlockIndicator, BlockCurveIndicator)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
OFFERS_INSERT_QUERY = """
    INSERT INTO OFFERS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                         EnergyOnlyOfferMW1, EnergyOnlyOfferPrice1, EnergyOnlyOfferMW2, EnergyOnlyOfferPrice2,
                         EnergyOnlyOfferMW3, EnergyOnlyOfferPrice3, EnergyOnlyOfferMW4, EnergyOnlyOfferPrice4,
                         EnergyOnlyOfferMW5, EnergyOnlyOfferPrice5, EnergyOnlyOfferMW6, EnergyOnlyOfferPrice6,
                         EnergyOnlyOfferMW7, EnergyOnlyOfferPrice7, EnergyOnlyOfferMW8, EnergyOnlyOfferPrice8,
                         EnergyOnlyOfferMW9, EnergyOnlyOfferPrice9, EnergyOnlyOfferMW10, EnergyOnlyOfferPrice10,
                         EnergyOnlyOfferID, MultiHourBlockIndicator, BlockCurveIndicator)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
OFFER_AWARDS_INSERT_QUERY = """
    INSERT INTO OFFER_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                              EnergyOnlyOfferAwardMW, SettlementPointPrice, OfferID)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

GET_ACTIVE_SETTLEMENT_POINTS_QUERY = """
    SELECT DISTINCT SettlementPoint FROM (
        SELECT SettlementPoint FROM BID_AWARDS
        UNION
        SELECT SettlementPoint FROM OFFER_AWARDS
    )
"""
FETCH_BID_SETTLEMENT_POINTS_QUERY = "SELECT SettlementPoint FROM BID_AWARDS"
CHECK_EXISTING_TABLES_QUERY = """
    SELECT name FROM sqlite_master
    WHERE type='table'
    AND name IN ('BID_AWARDS', 'OFFER_AWARDS')
"""
FETCH_OFFER_SETTLEMENT_POINTS_QUERY = "SELECT SettlementPoint FROM OFFER_AWARDS"

SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS SETTLEMENT_POINT_PRICES (
    DeliveryDate TEXT,
    DeliveryHour INTEGER,
    DeliveryInterval INTEGER,
    SettlementPointName TEXT,
    SettlementPointType TEXT,
    SettlementPointPrice REAL,
    DSTFlag TEXT
)
"""

BIDS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS BIDS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyBidMW1 REAL,
    EnergyOnlyBidPrice1 REAL,
    EnergyOnlyBidMW2 REAL,
    EnergyOnlyBidPrice2 REAL,
    EnergyOnlyBidMW3 REAL,
    EnergyOnlyBidPrice3 REAL,
    EnergyOnlyBidMW4 REAL,
    EnergyOnlyBidPrice4 REAL,
    EnergyOnlyBidMW5 REAL,
    EnergyOnlyBidPrice5 REAL,
    EnergyOnlyBidMW6 REAL,
    EnergyOnlyBidPrice6 REAL,
    EnergyOnlyBidMW7 REAL,
    EnergyOnlyBidPrice7 REAL,
    EnergyOnlyBidMW8 REAL,
    EnergyOnlyBidPrice8 REAL,
    EnergyOnlyBidMW9 REAL,
    EnergyOnlyBidPrice9 REAL,
    EnergyOnlyBidMW10 REAL,
    EnergyOnlyBidPrice10 REAL,
    EnergyOnlyBidID TEXT,
    MultiHourBlockIndicator TEXT,
    BlockCurveIndicator TEXT
)
"""

BID_AWARDS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS BID_AWARDS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyBidAwardMW REAL,
    SettlementPointPrice REAL,
    BidId TEXT
)
"""

OFFERS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS OFFERS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyOfferMW1 REAL,
    EnergyOnlyOfferPrice1 REAL,
    EnergyOnlyOfferMW2 REAL,
    EnergyOnlyOfferPrice2 REAL,
    EnergyOnlyOfferMW3 REAL,
    EnergyOnlyOfferPrice3 REAL,
    EnergyOnlyOfferMW4 REAL,
    EnergyOnlyOfferPrice4 REAL,
    EnergyOnlyOfferMW5 REAL,
    EnergyOnlyOfferPrice5 REAL,
    EnergyOnlyOfferMW6 REAL,
    EnergyOnlyOfferPrice6 REAL,
    EnergyOnlyOfferMW7 REAL,
    EnergyOnlyOfferPrice7 REAL,
    EnergyOnlyOfferMW8 REAL,
    EnergyOnlyOfferPrice8 REAL,
    EnergyOnlyOfferMW9 REAL,
    EnergyOnlyOfferPrice9 REAL,
    EnergyOnlyOfferMW10 REAL,
    EnergyOnlyOfferPrice10 REAL,
    EnergyOnlyOfferID TEXT,
    MultiHourBlockIndicator TEXT,
    BlockCurveIndicator TEXT
)
"""

OFFER_AWARDS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS OFFER_AWARDS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyOfferAwardMW REAL,
    SettlementPointPrice REAL,
    OfferID TEXT
)
"""

# New mappings for different CSV types
COLUMN_MAPPINGS = {
    "settlement_point_prices": {
        "deliverydate": "deliveryDate",
        "delivery_date": "deliveryDate",
        "deliveryhour": "deliveryHour",
        "delivery_hour": "deliveryHour",
        "deliveryinterval": "deliveryInterval",
        "delivery_interval": "deliveryInterval",
        "settlementPoint": "settlementPointName",
        "settlementpointname": "settlementPointName",
        "settlement_point_name": "settlementPointName",
        "settlementpointtype": "settlementPointType",
        "settlement_point_type": "settlementPointType",
        "settlementpointprice": "settlementPointPrice",
        "settlement_point_price": "settlementPointPrice",
        "dstflag": "dstFlag",
        "dst_flag": "dstFlag",
    },
    "bid_awards": {
        "delivery date": "deliveryDate",
        "delivery_date": "deliveryDate",
        "hour ending": "hourEnding",
        "hour_ending": "hourEnding",
        "settlement point": "settlementPointName",
        "settlement_point": "settlementPointName",
        "qse name": "qseName",
        "qse_name": "qseName",
        "energy only bid award in mw": "energyOnlyBidAwardInMW",
        "energy_only_bid_award_in_mw": "energyOnlyBidAwardInMW",
        "settlement point price": "settlementPointPrice",
        "settlement_point_price": "settlementPointPrice",
        "bid id": "bidId",
        "bid_id": "bidId",
    },
    "bids": {
        "delivery date": "deliveryDate",
        "delivery_date": "deliveryDate",
        "hour ending": "hourEnding",
        "hour_ending": "hourEnding",
        "settlement point": "settlementPointName",
        "settlement_point": "settlementPointName",
        "qse name": "qseName",
        "qse_name": "qseName",
        "energyOnlyOfferMW1": "energyOnlyBidMw1",
        "energy only bid mw1": "energyOnlyBidMw1",
        "energy_only_bid_mw1": "energyOnlyBidMw1",
        "energy only bid price1": "energyOnlyBidPrice1",
        "energy_only_bid_price1": "energyOnlyBidPrice1",
        "energyOnlyOfferMW2": "energyOnlyBidMw2",
        "energy only bid mw2": "energyOnlyBidMw2",
        "energy_only_bid_mw2": "energyOnlyBidMw2",
        "energyOnlyOfferMW2": "energyOnlyOfferMw2",
        "energy only bid price2": "energyOnlyBidPrice2",
        "energy_only_bid_price2": "energyOnlyBidPrice2",
        "energyOnlyOfferMW3": "energyOnlyBidMw3",
        "energy only bid mw3": "energyOnlyBidMw3",
        "energy_only_bid_mw3": "energyOnlyBidMw3",
        "energy only bid price3": "energyOnlyBidPrice3",
        "energy_only_bid_price3": "energyOnlyBidPrice3",
        "energyOnlyOfferMW4": "energyOnlyBidMw4",
        "energy only bid mw4": "energyOnlyBidMw4",
        "energy_only_bid_mw4": "energyOnlyBidMw4",
        "energy only bid price4": "energyOnlyBidPrice4",
        "energy_only_bid_price4": "energyOnlyBidPrice4",
        "energyOnlyOfferMW5": "energyOnlyBidMw5",
        "energy only bid mw5": "energyOnlyBidMw5",
        "energy_only_bid_mw5": "energyOnlyBidMw5",
        "energy only bid price5": "energyOnlyBidPrice5",
        "energy_only_bid_price5": "energyOnlyBidPrice5",
        "energyOnlyOfferMW6": "energyOnlyBidMw6",
        "energy only bid mw6": "energyOnlyBidMw6",
        "energy_only_bid_mw6": "energyOnlyBidMw6",
        "energy only bid price6": "energyOnlyBidPrice6",
        "energy_only_bid_price6": "energyOnlyBidPrice6",
        "energyOnlyOfferMW7": "energyOnlyBidMw7",
        "energy only bid mw7": "energyOnlyBidMw7",
        "energy_only_bid_mw7": "energyOnlyBidMw7",
        "energy only bid price7": "energyOnlyBidPrice7",
        "energy_only_bid_price7": "energyOnlyBidPrice7",
        "energyOnlyOfferMW8": "energyOnlyBidMw8",
        "energy only bid mw8": "energyOnlyBidMw8",
        "energy_only_bid_mw8": "energyOnlyBidMw8",
        "energy only bid price8": "energyOnlyBidPrice8",
        "energy_only_bid_price8": "energyOnlyBidPrice8",
        "energyOnlyOfferMW9": "energyOnlyBidMw9",
        "energy only bid mw9": "energyOnlyBidMw9",
        "energy_only_bid_mw9": "energyOnlyBidMw9",
        "energy only bid price9": "energyOnlyBidPrice9",
        "energy_only_bid_price9": "energyOnlyBidPrice9",
        "energyOnlyOfferMW10": "energyOnlyBidMw10",
        "energy only bid mw10": "energyOnlyBidMw10",
        "energy_only_bid_mw10": "energyOnlyBidMw10",
        "energy only bid price10": "energyOnlyBidPrice10",
        "energy_only_bid_price10": "energyOnlyBidPrice10",
        "energy only bid id": "bidId",
        "energy_only_bid_id": "bidId",
        "multi-hour block indicator": "multiHourBlock",
        "multi-hour_block_indicator": "multiHourBlock",
        "multi_hour_block_indicator": "multiHourBlock",
        "block/curve indicator": "blockCurve",
        "block_curve_indicator": "blockCurve",
        "block/curve_indicator": "blockCurve",
    },
    "offer_awards": {
        "delivery date": "deliveryDate",
        "delivery_date": "deliveryDate",
        "hour ending": "hourEnding",
        "hour_ending": "hourEnding",
        "settlement point": "settlementPointName",
        "settlement_point": "settlementPointName",
        "qse name": "qseName",
        "qse_name": "qseName",
        "energy only offer award in mw": "energyOnlyOfferAwardInMW",
        "energy_only_offer_award_in_mw": "energyOnlyOfferAwardInMW",
        "settlement point price": "settlementPointPrice",
        "settlement_point_price": "settlementPointPrice",
        "offer id": "offerId",
        "offer_id": "offerId",
    },
    "offers": {
        "delivery date": "deliveryDate",
        "delivery_date": "deliveryDate",
        "hour ending": "hourEnding",
        "hour_ending": "hourEnding",
        "settlement point": "settlementPointName",
        "settlement_point": "settlementPointName",
        "qse name": "qseName",
        "qse_name": "qseName",
        "energy only offer mw1": "energyOnlyOfferMW1",
        "energy_only_offer_mw1": "energyOnlyOfferMW1",
        "energyOnlyOfferMW1": "energyOnlyOfferMW1",
        "energy only offer price1": "energyOnlyOfferPrice1",
        "energy_only_offer_price1": "energyOnlyOfferPrice1",
        "energy only offer mw2": "energyOnlyOfferMW2",
        "energy_only_offer_mw2": "energyOnlyOfferMW2",
        "energy only offer price2": "energyOnlyOfferPrice2",
        "energy_only_offer_price2": "energyOnlyOfferPrice2",
        "energy only offer mw3": "energyOnlyOfferMW3",
        "energy_only_offer_mw3": "energyOnlyOfferMW3",
        "energy only offer price3": "energyOnlyOfferPrice3",
        "energy_only_offer_price3": "energyOnlyOfferPrice3",
        "energy only offer mw4": "energyOnlyOfferMW4",
        "energy_only_offer_mw4": "energyOnlyOfferMW4",
        "energy only offer price4": "energyOnlyOfferPrice4",
        "energy_only_offer_price4": "energyOnlyOfferPrice4",
        "energy only offer mw5": "energyOnlyOfferMW5",
        "energy_only_offer_mw5": "energyOnlyOfferMW5",
        "energy only offer price5": "energyOnlyOfferPrice5",
        "energy_only_offer_price5": "energyOnlyOfferPrice5",
        "energy only offer mw6": "energyOnlyOfferMW6",
        "energy_only_offer_mw6": "energyOnlyOfferMW6",
        "energy only offer price6": "energyOnlyOfferPrice6",
        "energy_only_offer_price6": "energyOnlyOfferPrice6",
        "energy only offer mw7": "energyOnlyOfferMW7",
        "energy_only_offer_mw7": "energyOnlyOfferMW7",
        "energy only offer price7": "energyOnlyOfferPrice7",
        "energy_only_offer_price7": "energyOnlyOfferPrice7",
        "energy only offer mw8": "energyOnlyOfferMW8",
        "energy_only_offer_mw8": "energyOnlyOfferMW8",
        "energy only offer price8": "energyOnlyOfferPrice8",
        "energy_only_offer_price8": "energyOnlyOfferPrice8",
        "energy only offer mw9": "energyOnlyOfferMW9",
        "energy_only_offer_mw9": "energyOnlyOfferMW9",
        "energy only offer price9": "energyOnlyOfferPrice9",
        "energy_only_offer_price9": "energyOnlyOfferPrice9",
        "energy only offer mw10": "energyOnlyOfferMW10",
        "energy_only_offer_mw10": "energyOnlyOfferMW10",
        "energy only offer price10": "energyOnlyOfferPrice10",
        "energy_only_offer_price10": "energyOnlyOfferPrice10",
        "energy only offer id": "offerId",
        "energy_only_offer_id": "offerId",
        "multi-hour_block_indicator": "multiHourBlock",
        "multi-hour block indicator": "multiHourBlock",
        "multi_hour_block_indicator": "multiHourBlock",
        "block/curve indicator": "blockCurve",
        "block_curve_indicator": "blockCurve",
        "block/curve_indicator": "blockCurve",
    },
}

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
        "insert_query": OFFER_AWARDS_INSERT_QUERY
    }
}
# Configure logging
LOGGER = logging.getLogger(__name__)

# Ensure environment variables are loaded correctly
LOGGER.info(f"ERCOT_API_BASE_URL_DAM: {ERCOT_API_BASE_URL_DAM}")
LOGGER.info(f"ERCOT_API_BASE_URL_SETTLEMENT: {ERCOT_API_BASE_URL_SETTLEMENT}")
