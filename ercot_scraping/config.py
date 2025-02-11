import os
from dotenv import load_dotenv
import requests

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
ERCOT_DATA_DB_FILE = "_data/ercot_data.db"
# CSV file path
QSE_FILTER_CSV = "_data/ERCOT_tracking_list.csv"

# Archive API endpoints
ERCOT_ARCHIVE_API_BASE_URL = "https://api.ercot.com/api/public-reports/archive"
ERCOT_ARCHIVE_PRODUCT_IDS = {
    "SPP": "NP6-788-CD",  # Settlement Point Prices
    "DAM_BIDS": "NP3-966-ER",  # DAM Energy Bids
    "DAM_BID_AWARDS": "NP3-966-ER",  # DAM Energy Bid Awards
    "DAM_OFFERS": "NP3-966-ER",  # DAM Energy Offers
    "DAM_OFFER_AWARDS": "NP3-966-ER",  # DAM Energy Offer Awards
}

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
