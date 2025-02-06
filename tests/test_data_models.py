import os
import pytest
from ercot_scraping.data_models import (
    SettlementPointPrice,
    Bid,
    BidAward,
    Offer,
    OfferAward,
)

TEST_DB = "test_ercot.db"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_settlement_point_price_as_tuple():
    spp = SettlementPointPrice(
        DeliveryDate="2023-10-01",
        DeliveryHour=10,
        DeliveryInterval=1,
        SettlementPointName="TestPoint",
        SettlementPointType="TypeA",
        SettlementPointPrice=50.0,
        DSTFlag="N",
    )
    expected = ("2023-10-01", 10, 1, "TestPoint", "TypeA", 50.0, "N")
    assert spp.as_tuple() == expected


def test_bid_as_tuple():
    bid = Bid(
        DeliveryDate="2023-10-01",
        HourEnding=11,
        SettlementPoint="PointA",
        QSEName="Entity1",
        EnergyOnlyBidMW1=100.0,
        EnergyOnlyBidPrice1=45.0,
        EnergyOnlyBidMW2=200.0,
        EnergyOnlyBidPrice2=55.0,
        EnergyOnlyBidMW3=150.0,
        EnergyOnlyBidPrice3=50.0,
        EnergyOnlyBidMW4=250.0,
        EnergyOnlyBidPrice4=60.0,
        EnergyOnlyBidMW5=None,
        EnergyOnlyBidPrice5=None,
        EnergyOnlyBidMW6=None,
        EnergyOnlyBidPrice6=None,
        EnergyOnlyBidMW7=None,
        EnergyOnlyBidPrice7=None,
        EnergyOnlyBidMW8=None,
        EnergyOnlyBidPrice8=None,
        EnergyOnlyBidMW9=None,
        EnergyOnlyBidPrice9=None,
        EnergyOnlyBidMW10=None,
        EnergyOnlyBidPrice10=None,
        EnergyOnlyBidID="BidID123",
        MultiHourBlockIndicator="Y",
        BlockCurveIndicator="N",
    )
    expected = (
        "2023-10-01",
        11,
        "PointA",
        "Entity1",
        100.0,
        45.0,
        200.0,
        55.0,
        150.0,
        50.0,
        250.0,
        60.0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "BidID123",
        "Y",
        "N",
    )
    assert bid.as_tuple() == expected


def test_bid_award_as_tuple():
    bid_award = BidAward(
        DeliveryDate="2023-10-02",
        HourEnding=12,
        SettlementPoint="PointB",
        QSEName="Entity2",
        EnergyOnlyBidAwardMW=300.0,
        SettlementPointPrice=65.0,
        BidID="BA123",
    )
    expected = (
        "2023-10-02",
        12,
        "PointB",
        "Entity2",
        300.0,
        65.0,
        "BA123",
    )
    assert bid_award.as_tuple() == expected


def test_offer_as_tuple():
    offer = Offer(
        DeliveryDate="2023-10-03",
        HourEnding=13,
        SettlementPoint="PointC",
        QSEName="Entity3",
        EnergyOnlyOfferMW1=120.0,
        EnergyOnlyOfferPrice1=40.0,
        EnergyOnlyOfferMW2=130.0,
        EnergyOnlyOfferPrice2=42.0,
        EnergyOnlyOfferMW3=None,
        EnergyOnlyOfferPrice3=None,
        EnergyOnlyOfferMW4=None,
        EnergyOnlyOfferPrice4=None,
        EnergyOnlyOfferMW5=None,
        EnergyOnlyOfferPrice5=None,
        EnergyOnlyOfferMW6=None,
        EnergyOnlyOfferPrice6=None,
        EnergyOnlyOfferMW7=None,
        EnergyOnlyOfferPrice7=None,
        EnergyOnlyOfferMW8=None,
        EnergyOnlyOfferPrice8=None,
        EnergyOnlyOfferMW9=None,
        EnergyOnlyOfferPrice9=None,
        EnergyOnlyOfferMW10=None,
        EnergyOnlyOfferPrice10=None,
        EnergyOnlyOfferID="OfferID456",
        MultiHourBlockIndicator="N",
        BlockCurveIndicator="Y",
    )
    expected = (
        "2023-10-03",  # DeliveryDate
        13,  # HourEnding
        "PointC",  # SettlementPoint
        "Entity3",  # QSEName
        120.0,  # EnergyOnlyOfferMW1
        40.0,  # EnergyOnlyOfferPrice1
        130.0,  # EnergyOnlyOfferMW2
        42.0,  # EnergyOnlyOfferPrice2
        None,  # EnergyOnlyOfferMW3
        None,  # EnergyOnlyOfferPrice3
        None,  # EnergyOnlyOfferMW4
        None,  # EnergyOnlyOfferPrice4
        None,  # EnergyOnlyOfferMW5
        None,  # EnergyOnlyOfferPrice5
        None,  # EnergyOnlyOfferMW6
        None,  # EnergyOnlyOfferPrice6
        None,  # EnergyOnlyOfferMW7
        None,  # EnergyOnlyOfferPrice7
        None,  # EnergyOnlyOfferMW8
        None,  # EnergyOnlyOfferPrice8
        None,  # EnergyOnlyOfferMW9
        None,  # EnergyOnlyOfferPrice9
        None,  # EnergyOnlyOfferMW10
        None,  # EnergyOnlyOfferPrice10
        "OfferID456",  # EnergyOnlyOfferID
        "N",  # MultiHourBlockIndicator
        "Y",  # BlockCurveIndicator
    )
    assert offer.as_tuple() == expected


def test_offer_award_as_tuple():
    offer_award = OfferAward(
        DeliveryDate="2023-10-04",
        HourEnding=14,
        SettlementPoint="PointD",
        QSEName="Entity4",
        EnergyOnlyOfferAwardMW=350.0,
        SettlementPointPrice=70.0,
        OfferID="OA789",
    )
    expected = (
        "2023-10-04",
        14,
        "PointD",
        "Entity4",
        350.0,
        70.0,
        "OA789",
    )
    assert offer_award.as_tuple() == expected
