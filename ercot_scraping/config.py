import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ERCOT API configuration
ERCOT_API_BASE_URL_DAM = os.getenv("ERCOT_API_BASE_URL_DAM")
ERCOT_API_BASE_URL_SETTLEMENT = os.getenv("ERCOT_API_BASE_URL_SETTLEMENT")
ERCOT_API_SUBSCRIPTION_KEY = os.getenv("ERCOT_API_SUBSCRIPTION_KEY")
ERCOT_API_REQUEST_HEADERS = {"Ocp-Apim-Subscription-Key": ERCOT_API_SUBSCRIPTION_KEY}
ERCOT_USERNAME = os.getenv("ERCOT_API_USERNAME")
ERCOT_PASSWORD = os.getenv("ERCOT_API_PASSWORD")
AUTH_URL = """https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token?username={username}&password={password}&grant_type=password&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70&response_type=id_token"""

ERCOT_ID_TOKEN = os.getenv("ERCOT_ID_TOKEN")

# SQL Queries
SETTLEMENT_POINT_PRICES_INSERT_QUERY = """
    INSERT INTO SETTLEMENT_POINT_PRICES (DeliveryDate, DeliveryHour, DeliveryInterval,
                                         SettlementPointName, SettlementPointType,
                                         SettlementPointPrice, DSTFlag)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
BID_AWARDS_INSERT_QUERY = """
    INSERT INTO BID_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName, 
                            EnergyOnlyBidAwardMW, SettlementPointPrice, BidID)
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
