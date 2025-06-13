from ercot_scraping.database import data_models


class TestDataModels:
    def test_hello_world(self):
        assert True

    def test_settlement_point_price_as_tuple(self):
        spp = data_models.SettlementPointPrice(
            deliveryDate="2024-06-01",
            deliveryHour=12,
            deliveryInterval=3,
            settlementPointName="POINT1",
            settlementPointType="TYPE1",
            settlementPointPrice=42.5,
            dstFlag="N",
            inserted_at="2024-06-01T12:00:00"
        )
        expected = (
            "2024-06-01", 12, 3, "POINT1", "TYPE1", 42.5, "N", "2024-06-01T12:00:00"
        )
        assert spp.as_tuple() == expected

    def test_bid_as_tuple_and_post_init(self):
        bid = data_models.Bid(
            deliveryDate="2024-06-01",
            hourEnding=14,
            settlementPointName="POINT2",
            qseName="QSE1",
            energyOnlyBidMw1=10.0,
            energyOnlyBidPrice1=30.5,
            bidId=123,
            multiHourBlock="Y",
            blockCurve="N",
            inserted_at="2024-06-01T14:00:00"
        )
        # bidId should be converted to string
        assert isinstance(bid.bidId, str)
        expected = (
            "2024-06-01", 14, "POINT2", "QSE1",
            10.0, 30.5, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None,
            "123", "Y", "N", "2024-06-01T14:00:00"
        )
        assert bid.as_tuple() == expected

    def test_bid_award_as_tuple_and_post_init(self):
        award = data_models.BidAward(
            deliveryDate="2024-06-02",
            hourEnding="15",
            settlementPointName="POINT3",
            qseName="QSE2",
            energyOnlyBidAwardInMW="25.0",
            settlementPointPrice="50.5",
            bidId=456,
            inserted_at="2024-06-02T15:00:00"
        )
        # hourEnding, energyOnlyBidAwardInMW, settlementPointPrice should be converted to correct types
        assert isinstance(award.hourEnding, int)
        assert isinstance(award.energyOnlyBidAwardInMW, float)
        assert isinstance(award.settlementPointPrice, float)
        assert isinstance(award.bidId, str)
        expected = (
            "2024-06-02", 15, "POINT3", "QSE2", 25.0, 50.5, "456", "2024-06-02T15:00:00"
        )
        assert award.as_tuple() == expected

    def test_offer_as_tuple_and_post_init(self):
        offer = data_models.Offer(
            deliveryDate="2024-06-03",
            hourEnding=16,
            settlementPointName="POINT4",
            qseName="QSE3",
            energyOnlyOfferMW1=5.0,
            energyOnlyOfferPrice1=20.0,
            offerId=789,
            multiHourBlock="N",
            blockCurve="Y",
            inserted_at="2024-06-03T16:00:00"
        )
        # offerId should be converted to string
        assert isinstance(offer.offerId, str)
        expected = (
            "2024-06-03", 16, "POINT4", "QSE3",
            5.0, 20.0, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None,
            "789", "N", "Y", "2024-06-03T16:00:00"
        )
        assert offer.as_tuple() == expected
