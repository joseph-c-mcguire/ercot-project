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

    def test_bid_schema_validation(self):
        from ercot_scraping.database.data_models import BidSchema
        valid = {
            "deliveryDate": "2024-06-01",
            "hourEnding": 1,
            "settlementPointName": "POINT1",
            "qseName": "QSE1",
            "energyOnlyBidMw1": 10.0,
            "energyOnlyBidPrice1": 20.0,
            "bidId": "BID1",
            "multiHourBlock": "N",
            "blockCurve": "N"
        }
        BidSchema(**valid)  # Should not raise
        # Missing required field
        import pytest
        with pytest.raises(Exception):
            BidSchema(**{k: v for k, v in valid.items()
                      if k != "deliveryDate"})

    def test_bid_award_schema_validation(self):
        from ercot_scraping.database.data_models import BidAwardSchema
        valid = {
            "deliveryDate": "2024-06-01",
            "hourEnding": 1,
            "settlementPointName": "POINT1",
            "qseName": "QSE1",
            "energyOnlyBidAwardInMW": 10.0,
            "settlementPointPrice": 30.5,
            "bidId": "BID1"
        }
        BidAwardSchema(**valid)  # Should not raise
        import pytest
        with pytest.raises(Exception):
            BidAwardSchema(**{k: v for k, v in valid.items() if k != "bidId"})
