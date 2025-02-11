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
        deliveryDate="2023-10-01",
        deliveryHour=10,
        deliveryInterval=1,
        settlementPointName="TestPoint",
        settlementPointType="TypeA",
        settlementPointPrice=50.0,
        dstFlag="N",
    )
    expected = ("2023-10-01", 10, 1, "TestPoint", "TypeA", 50.0, "N")
    assert spp.as_tuple() == expected


def test_bid_as_tuple():
    bid = Bid(
        deliveryDate="2023-10-01",
        hourEnding=11,
        settlementPointName="PointA",
        qseName="Entity1",
        energyOnlyBidMw1=100.0,
        energyOnlyBidPrice1=45.0,
        energyOnlyBidMw2=200.0,
        energyOnlyBidPrice2=55.0,
        energyOnlyBidMw3=150.0,
        energyOnlyBidPrice3=50.0,
        energyOnlyBidMw4=250.0,
        energyOnlyBidPrice4=60.0,
        energyOnlyBidMw5=None,
        energyOnlyBidPrice5=None,
        energyOnlyBidMw6=None,
        energyOnlyBidPrice6=None,
        energyOnlyBidMw7=None,
        energyOnlyBidPrice7=None,
        energyOnlyBidMw8=None,
        energyOnlyBidPrice8=None,
        energyOnlyBidMw9=None,
        energyOnlyBidPrice9=None,
        energyOnlyBidMw10=None,
        energyOnlyBidPrice10=None,
        bidId="BidID123",
        multiHourBlock="Y",
        blockCurve="N",
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
    bidAward = BidAward(
        deliveryDate="2023-10-02",
        hourEnding=12,
        settlementPointName="PointB",
        qseName="Entity2",
        energyOnlyBidAwardInMW=300.0,
        settlementPointPrice=65.0,
        bidId="BA123",
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
    assert bidAward.as_tuple() == expected


def test_offer_as_tuple():
    offer = Offer(
        deliveryDate="2023-10-03",
        hourEnding=13,
        settlementPointName="PointC",
        qseName="Entity3",
        energyOnlyOfferMW1=120.0,
        energyOnlyOfferPrice1=40.0,
        energyOnlyOfferMW2=130.0,
        energyOnlyOfferPrice2=42.0,
        energyOnlyOfferMW3=None,
        energyOnlyOfferPrice3=None,
        energyOnlyOfferMW4=None,
        energyOnlyOfferPrice4=None,
        energyOnlyOfferMW5=None,
        energyOnlyOfferPrice5=None,
        energyOnlyOfferMW6=None,
        energyOnlyOfferPrice6=None,
        energyOnlyOfferMW7=None,
        energyOnlyOfferPrice7=None,
        energyOnlyOfferMW8=None,
        energyOnlyOfferPrice8=None,
        energyOnlyOfferMW9=None,
        energyOnlyOfferPrice9=None,
        energyOnlyOfferMW10=None,
        energyOnlyOfferPrice10=None,
        offerId="OfferID456",
        multiHourBlock="N",
        blockCurve="Y",
    )
    expected = (
        "2023-10-03",  # deliveryDate
        13,  # hourEnding
        "PointC",  # settlementPoint
        "Entity3",  # qseName
        120.0,  # energyOnlyOfferMw1
        40.0,  # energyOnlyOfferPrice1
        130.0,  # energyOnlyOfferMw2
        42.0,  # energyOnlyOfferPrice2
        None,  # energyOnlyOfferMw3
        None,  # energyOnlyOfferPrice3
        None,  # energyOnlyOfferMw4
        None,  # energyOnlyOfferPrice4
        None,  # energyOnlyOfferMw5
        None,  # energyOnlyOfferPrice5
        None,  # energyOnlyOfferMw6
        None,  # energyOnlyOfferPrice6
        None,  # energyOnlyOfferMw7
        None,  # energyOnlyOfferPrice7
        None,  # energyOnlyOfferMw8
        None,  # energyOnlyOfferPrice8
        None,  # energyOnlyOfferMw9
        None,  # energyOnlyOfferPrice9
        None,  # energyOnlyOfferMw10
        None,  # energyOnlyOfferPrice10
        "OfferID456",  # offerId
        "N",  # multiHourBlock
        "Y",  # blockCurveIndicator
    )
    assert offer.as_tuple() == expected


def test_offer_award_as_tuple():
    offerAward = OfferAward(
        deliveryDate="2023-10-04",
        hourEnding=14,
        settlementPointName="PointD",
        qseName="Entity4",
        energyOnlyOfferAwardInMW=350.0,
        settlementPointPrice=70.0,
        offerId="OA789",
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
    assert offerAward.as_tuple() == expected
