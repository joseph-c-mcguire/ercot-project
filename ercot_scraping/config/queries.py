# Table Creation Queries
SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS SETTLEMENT_POINT_PRICES (
    DeliveryDate TEXT,
    DeliveryHour INTEGER,
    DeliveryInterval INTEGER,
    SettlementPointName TEXT,
    SettlementPointType TEXT,
    SettlementPointPrice REAL,
    DSTFlag TEXT,
    INSERTED_AT TEXT DEFAULT (datetime('now'))
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
    BlockCurveIndicator TEXT,
    INSERTED_AT TEXT DEFAULT (datetime('now'))
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
    BidId TEXT,
    INSERTED_AT TEXT DEFAULT (datetime('now'))
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
    BlockCurveIndicator TEXT,
    INSERTED_AT TEXT DEFAULT (datetime('now'))
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
    OfferID TEXT,
    INSERTED_AT TEXT DEFAULT (datetime('now'))
)
"""

CREATE_FINAL_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS FINAL (
    deliveryDate TEXT,
    hourEnding INTEGER,
    settlementPointName TEXT,
    qseName TEXT,
    energyOnlyBidAwardInMW REAL,
    settlementPointPrice REAL,
    bidId TEXT,
    MARK_PRICE REAL,
    BID_PRICE REAL,
    BID_SIZE REAL,
    blockCurve TEXT
)
"""

# Insert Queries
SETTLEMENT_POINT_PRICES_INSERT_QUERY = """
    INSERT INTO SETTLEMENT_POINT_PRICES (DeliveryDate, DeliveryHour, DeliveryInterval,
                                         SettlementPointName, SettlementPointType,
                                         SettlementPointPrice, DSTFlag, INSERTED_AT)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

BID_AWARDS_INSERT_QUERY = """
    INSERT INTO BID_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                            EnergyOnlyBidAwardMW, SettlementPointPrice, BidId, INSERTED_AT)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

BIDS_INSERT_QUERY = """
    INSERT INTO BIDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                      EnergyOnlyBidMW1, EnergyOnlyBidPrice1, EnergyOnlyBidMW2, EnergyOnlyBidPrice2,
                      EnergyOnlyBidMW3, EnergyOnlyBidPrice3, EnergyOnlyBidMW4, EnergyOnlyBidPrice4,
                      EnergyOnlyBidMW5, EnergyOnlyBidPrice5, EnergyOnlyBidMW6, EnergyOnlyBidPrice6,
                      EnergyOnlyBidMW7, EnergyOnlyBidPrice7, EnergyOnlyBidMW8, EnergyOnlyBidPrice8,
                      EnergyOnlyBidMW9, EnergyOnlyBidPrice9, EnergyOnlyBidMW10, EnergyOnlyBidPrice10,
                      EnergyOnlyBidID, MultiHourBlockIndicator, BlockCurveIndicator, INSERTED_AT)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

OFFERS_INSERT_QUERY = """
    INSERT INTO OFFERS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                         EnergyOnlyOfferMW1, EnergyOnlyOfferPrice1, EnergyOnlyOfferMW2, EnergyOnlyOfferPrice2,
                         EnergyOnlyOfferMW3, EnergyOnlyOfferPrice3, EnergyOnlyOfferMW4, EnergyOnlyOfferPrice4,
                         EnergyOnlyOfferMW5, EnergyOnlyOfferPrice5, EnergyOnlyOfferMW6, EnergyOnlyOfferPrice6,
                         EnergyOnlyOfferMW7, EnergyOnlyOfferPrice7, EnergyOnlyOfferMW8, EnergyOnlyOfferPrice8,
                         EnergyOnlyOfferMW9, EnergyOnlyOfferPrice9, EnergyOnlyOfferMW10, EnergyOnlyOfferPrice10,
                         EnergyOnlyOfferID, MultiHourBlockIndicator, BlockCurveIndicator, INSERTED_AT)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

OFFER_AWARDS_INSERT_QUERY = """
    INSERT INTO OFFER_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                              EnergyOnlyOfferAwardMW, SettlementPointPrice, OfferID, INSERTED_AT)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

# Select Queries
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

# Merge Query
MERGE_DATA_QUERY = """
INSERT INTO FINAL
SELECT 
    ba.DeliveryDate,
    ba.HourEnding,
    ba.SettlementPoint as settlementPointName,
    ba.QSEName,
    ba.EnergyOnlyBidAwardMW as energyOnlyBidAwardInMW,
    COALESCE(spp.SettlementPointPrice, ba.SettlementPointPrice) as settlementPointPrice,
    ba.BidId,
    spp.SettlementPointPrice as MARK_PRICE,
    CASE 
        WHEN b.EnergyOnlyBidMW1 IS NOT NULL THEN b.EnergyOnlyBidPrice1
        WHEN b.EnergyOnlyBidMW2 IS NOT NULL THEN b.EnergyOnlyBidPrice2
        WHEN b.EnergyOnlyBidMW3 IS NOT NULL THEN b.EnergyOnlyBidPrice3
        WHEN b.EnergyOnlyBidMW4 IS NOT NULL THEN b.EnergyOnlyBidPrice4
        WHEN b.EnergyOnlyBidMW5 IS NOT NULL THEN b.EnergyOnlyBidPrice5
        ELSE NULL
    END as BID_PRICE,
    CASE 
        WHEN b.EnergyOnlyBidMW1 IS NOT NULL THEN b.EnergyOnlyBidMW1
        WHEN b.EnergyOnlyBidMW2 IS NOT NULL THEN b.EnergyOnlyBidMW2
        WHEN b.EnergyOnlyBidMW3 IS NOT NULL THEN b.EnergyOnlyBidMW3
        WHEN b.EnergyOnlyBidMW4 IS NOT NULL THEN b.EnergyOnlyBidMW4
        WHEN b.EnergyOnlyBidMW5 IS NOT NULL THEN b.EnergyOnlyBidMW5
        ELSE NULL
    END as BID_SIZE,
    b.BlockCurveIndicator as blockCurve
FROM BID_AWARDS ba
LEFT JOIN BIDS b ON ba.BidId = b.EnergyOnlyBidID 
    AND ba.DeliveryDate = b.DeliveryDate 
    AND ba.HourEnding = b.HourEnding
LEFT JOIN SETTLEMENT_POINT_PRICES spp ON ba.SettlementPoint = spp.SettlementPointName 
    AND ba.DeliveryDate = spp.DeliveryDate 
    AND ba.HourEnding = spp.DeliveryHour
"""
