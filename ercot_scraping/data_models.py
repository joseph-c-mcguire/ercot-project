from dataclasses import dataclass
from typing import Optional


@dataclass
class SettlementPointPrice:
    DeliveryDate: str
    DeliveryHour: int
    DeliveryInterval: int
    SettlementPointName: str
    SettlementPointType: str
    SettlementPointPrice: float
    DSTFlag: str

    def as_tuple(self):
        return (
            self.DeliveryDate,
            self.DeliveryHour,
            self.DeliveryInterval,
            self.SettlementPointName,
            self.SettlementPointType,
            self.SettlementPointPrice,
            self.DSTFlag,
        )


@dataclass
class Bid:
    DeliveryDate: str
    HourEnding: int
    SettlementPoint: str
    QSEName: str
    EnergyOnlyBidMW1: Optional[float] = None
    EnergyOnlyBidPrice1: Optional[float] = None
    EnergyOnlyBidMW2: Optional[float] = None
    EnergyOnlyBidPrice2: Optional[float] = None
    EnergyOnlyBidMW3: Optional[float] = None
    EnergyOnlyBidPrice3: Optional[float] = None
    EnergyOnlyBidMW4: Optional[float] = None
    EnergyOnlyBidPrice4: Optional[float] = None
    EnergyOnlyBidMW5: Optional[float] = None
    EnergyOnlyBidPrice5: Optional[float] = None
    EnergyOnlyBidMW6: Optional[float] = None
    EnergyOnlyBidPrice6: Optional[float] = None
    EnergyOnlyBidMW7: Optional[float] = None
    EnergyOnlyBidPrice7: Optional[float] = None
    EnergyOnlyBidMW8: Optional[float] = None
    EnergyOnlyBidPrice8: Optional[float] = None
    EnergyOnlyBidMW9: Optional[float] = None
    EnergyOnlyBidPrice9: Optional[float] = None
    EnergyOnlyBidMW10: Optional[float] = None
    EnergyOnlyBidPrice10: Optional[float] = None
    EnergyOnlyBidID: Optional[str] = None
    MultiHourBlockIndicator: Optional[str] = None
    BlockCurveIndicator: Optional[str] = None

    def as_tuple(self):
        return (
            self.DeliveryDate,
            self.HourEnding,
            self.SettlementPoint,
            self.QSEName,
            self.EnergyOnlyBidMW1,
            self.EnergyOnlyBidPrice1,
            self.EnergyOnlyBidMW2,
            self.EnergyOnlyBidPrice2,
            self.EnergyOnlyBidMW3,
            self.EnergyOnlyBidPrice3,
            self.EnergyOnlyBidMW4,
            self.EnergyOnlyBidPrice4,
            self.EnergyOnlyBidMW5,
            self.EnergyOnlyBidPrice5,
            self.EnergyOnlyBidMW6,
            self.EnergyOnlyBidPrice6,
            self.EnergyOnlyBidMW7,
            self.EnergyOnlyBidPrice7,
            self.EnergyOnlyBidMW8,
            self.EnergyOnlyBidPrice8,
            self.EnergyOnlyBidMW9,
            self.EnergyOnlyBidPrice9,
            self.EnergyOnlyBidMW10,
            self.EnergyOnlyBidPrice10,
            self.EnergyOnlyBidID,
            self.MultiHourBlockIndicator,
            self.BlockCurveIndicator,
        )


@dataclass
class BidAward:
    DeliveryDate: str
    HourEnding: int
    SettlementPoint: str
    QSEName: str
    EnergyOnlyBidAwardMW: float
    SettlementPointPrice: float
    BidID: str

    def as_tuple(self):
        return (
            self.DeliveryDate,
            self.HourEnding,
            self.SettlementPoint,
            self.QSEName,
            self.EnergyOnlyBidAwardMW,
            self.SettlementPointPrice,
            self.BidID,
        )


@dataclass
class Offer:
    DeliveryDate: str
    HourEnding: int
    SettlementPoint: str
    QSEName: str
    EnergyOnlyOfferMW1: Optional[float] = None
    EnergyOnlyOfferPrice1: Optional[float] = None
    EnergyOnlyOfferMW2: Optional[float] = None
    EnergyOnlyOfferPrice2: Optional[float] = None
    EnergyOnlyOfferMW3: Optional[float] = None
    EnergyOnlyOfferPrice3: Optional[float] = None
    EnergyOnlyOfferMW4: Optional[float] = None
    EnergyOnlyOfferPrice4: Optional[float] = None
    EnergyOnlyOfferMW5: Optional[float] = None
    EnergyOnlyOfferPrice5: Optional[float] = None
    EnergyOnlyOfferMW6: Optional[float] = None
    EnergyOnlyOfferPrice6: Optional[float] = None
    EnergyOnlyOfferMW7: Optional[float] = None
    EnergyOnlyOfferPrice7: Optional[float] = None
    EnergyOnlyOfferMW8: Optional[float] = None
    EnergyOnlyOfferPrice8: Optional[float] = None
    EnergyOnlyOfferMW9: Optional[float] = None
    EnergyOnlyOfferPrice9: Optional[float] = None
    EnergyOnlyOfferMW10: Optional[float] = None
    EnergyOnlyOfferPrice10: Optional[float] = None
    EnergyOnlyOfferID: Optional[str] = None
    MultiHourBlockIndicator: Optional[str] = None
    BlockCurveIndicator: Optional[str] = None

    def as_tuple(self):
        return (
            self.DeliveryDate,
            self.HourEnding,
            self.SettlementPoint,
            self.QSEName,
            self.EnergyOnlyOfferMW1,
            self.EnergyOnlyOfferPrice1,
            self.EnergyOnlyOfferMW2,
            self.EnergyOnlyOfferPrice2,
            self.EnergyOnlyOfferMW3,
            self.EnergyOnlyOfferPrice3,
            self.EnergyOnlyOfferMW4,
            self.EnergyOnlyOfferPrice4,
            self.EnergyOnlyOfferMW5,
            self.EnergyOnlyOfferPrice5,
            self.EnergyOnlyOfferMW6,
            self.EnergyOnlyOfferPrice6,
            self.EnergyOnlyOfferMW7,
            self.EnergyOnlyOfferPrice7,
            self.EnergyOnlyOfferMW8,
            self.EnergyOnlyOfferPrice8,
            self.EnergyOnlyOfferMW9,
            self.EnergyOnlyOfferPrice9,
            self.EnergyOnlyOfferMW10,
            self.EnergyOnlyOfferPrice10,
            self.EnergyOnlyOfferID,
            self.MultiHourBlockIndicator,
            self.BlockCurveIndicator,
        )


@dataclass
class OfferAward:
    DeliveryDate: str
    HourEnding: int
    SettlementPoint: str
    QSEName: str
    EnergyOnlyOfferAwardMW: float
    SettlementPointPrice: float
    OfferID: str

    def as_tuple(self):
        return (
            self.DeliveryDate,
            self.HourEnding,
            self.SettlementPoint,
            self.QSEName,
            self.EnergyOnlyOfferAwardMW,
            self.SettlementPointPrice,
            self.OfferID,
        )
