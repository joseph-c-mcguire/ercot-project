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
    settlementPointPrice REAL,
    MARK_PRICE REAL,
    blockCurve TEXT,
    sourceType TEXT,
    energyOnlyBidAwardInMW REAL,
    bidId TEXT,
    BID_PRICE REAL,
    BID_SIZE REAL,
    energyOnlyOfferAwardMW REAL,
    offerId TEXT,
    OFFER_PRICE REAL,
    OFFER_SIZE REAL,
    INSERTED_AT TEXT DEFAULT (datetime('now'))
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
INSERT INTO FINAL (
    deliveryDate,
    hourEnding,
    settlementPointName,
    qseName,
    settlementPointPrice,
    MARK_PRICE,
    blockCurve,
    sourceType,
    energyOnlyBidAwardInMW,
    bidId,
    BID_PRICE,
    BID_SIZE,
    energyOnlyOfferAwardMW,
    offerId,
    OFFER_PRICE,
    OFFER_SIZE,
    INSERTED_AT
)
SELECT 
    ba.deliveryDate,
    ba.HourEnding,
    ba.SettlementPoint as settlementPointName,
    ba.QSEName,
    COALESCE(spp.SettlementPointPrice, ba.SettlementPointPrice) as settlementPointPrice,
    spp.SettlementPointPrice as MARK_PRICE,
    b.BlockCurveIndicator as blockCurve,
    'Bid' as sourceType,
    ba.EnergyOnlyBidAwardMW as energyOnlyBidAwardInMW,
    ba.BidId,
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
    NULL as energyOnlyOfferAwardMW,
    NULL as offerId,
    NULL as OFFER_PRICE,
    NULL as OFFER_SIZE,
    datetime('now') as INSERTED_AT
FROM BID_AWARDS ba
LEFT JOIN BIDS b ON ba.BidId = b.EnergyOnlyBidID 
    AND ba.DeliveryDate = b.DeliveryDate 
    AND ba.HourEnding = b.HourEnding
LEFT JOIN SETTLEMENT_POINT_PRICES spp ON ba.SettlementPoint = spp.SettlementPointName 
    AND ba.DeliveryDate = spp.DeliveryDate 
    AND ba.HourEnding = spp.DeliveryHour

UNION ALL

SELECT 
    oa.DeliveryDate,
    oa.HourEnding,
    oa.SettlementPoint as settlementPointName,
    oa.QSEName,
    COALESCE(spp.SettlementPointPrice, oa.SettlementPointPrice) as settlementPointPrice,
    spp.SettlementPointPrice as MARK_PRICE,
    o.BlockCurveIndicator as blockCurve,
    'Offer' as sourceType,
    NULL as energyOnlyBidAwardInMW,
    NULL as bidId,
    NULL as BID_PRICE,
    NULL as BID_SIZE,
    oa.EnergyOnlyOfferAwardMW as energyOnlyOfferAwardMW,
    oa.OfferID as offerId,
    CASE 
        WHEN o.EnergyOnlyOfferMW1 IS NOT NULL THEN o.EnergyOnlyOfferPrice1
        WHEN o.EnergyOnlyOfferMW2 IS NOT NULL THEN o.EnergyOnlyOfferPrice2
        WHEN o.EnergyOnlyOfferMW3 IS NOT NULL THEN o.EnergyOnlyOfferPrice3
        WHEN o.EnergyOnlyOfferMW4 IS NOT NULL THEN o.EnergyOnlyOfferPrice4
        WHEN o.EnergyOnlyOfferMW5 IS NOT NULL THEN o.EnergyOnlyOfferPrice5
        ELSE NULL
    END as OFFER_PRICE,
    CASE 
        WHEN o.EnergyOnlyOfferMW1 IS NOT NULL THEN o.EnergyOnlyOfferMW1
        WHEN o.EnergyOnlyOfferMW2 IS NOT NULL THEN o.EnergyOnlyOfferMW2
        WHEN o.EnergyOnlyOfferMW3 IS NOT NULL THEN o.EnergyOnlyOfferMW3
        WHEN o.EnergyOnlyOfferMW4 IS NOT NULL THEN o.EnergyOnlyOfferMW4
        WHEN o.EnergyOnlyOfferMW5 IS NOT NULL THEN o.EnergyOnlyOfferMW5
        ELSE NULL
    END as OFFER_SIZE,
    datetime('now') as INSERTED_AT
FROM OFFER_AWARDS oa
LEFT JOIN OFFERS o ON oa.OfferID = o.EnergyOnlyOfferID 
    AND oa.DeliveryDate = o.DeliveryDate 
    AND oa.HourEnding = o.HourEnding
LEFT JOIN SETTLEMENT_POINT_PRICES spp ON oa.SettlementPoint = spp.SettlementPointName 
    AND oa.DeliveryDate = spp.DeliveryDate 
    AND oa.HourEnding = spp.DeliveryHour
"""
