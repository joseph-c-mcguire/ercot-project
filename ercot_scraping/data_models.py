from dataclasses import dataclass
from typing import Optional


@dataclass
class SettlementPointPrice:
    """
    Represents settlement price details for a delivery period.
    Attributes:
        DeliveryDate (str): The date when the delivery occurs.
        DeliveryHour (int): The hour of delivery.
        DeliveryInterval (int): The specific interval within the delivery hour.
        SettlementPointName (str): The name of the settlement point.
        SettlementPointType (str): The type or category of the settlement point.
        SettlementPointPrice (float): The price at the settlement point.
        DSTFlag (str): Indicator flag for Daylight Saving Time.
    Methods:
        as_tuple():
            Returns:
                tuple: A tuple containing all the settlement point price attributes in the following order:
                    (DeliveryDate, DeliveryHour, DeliveryInterval, SettlementPointName,
                     SettlementPointType, SettlementPointPrice, DSTFlag)
    """

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
    """
    Represents a bid in the ERCOT energy market.
    Attributes:
        DeliveryDate (str): The delivery date of the bid.
        HourEnding (int): The hour that marks the end of the delivery period.
        SettlementPoint (str): The identifier for the settlement point related to the bid.
        QSEName (str): The name of the Qualified Scheduling Entity (QSE).
        EnergyOnlyBidMW1 (Optional[float]): Megawatt quantity for the first energy-only bid level.
        EnergyOnlyBidPrice1 (Optional[float]): Bid price for the first energy-only bid level.
        EnergyOnlyBidMW2 (Optional[float]): Megawatt quantity for the second energy-only bid level.
        EnergyOnlyBidPrice2 (Optional[float]): Bid price for the second energy-only bid level.
        EnergyOnlyBidMW3 (Optional[float]): Megawatt quantity for the third energy-only bid level.
        EnergyOnlyBidPrice3 (Optional[float]): Bid price for the third energy-only bid level.
        EnergyOnlyBidMW4 (Optional[float]): Megawatt quantity for the fourth energy-only bid level.
        EnergyOnlyBidPrice4 (Optional[float]): Bid price for the fourth energy-only bid level.
        EnergyOnlyBidMW5 (Optional[float]): Megawatt quantity for the fifth energy-only bid level.
        EnergyOnlyBidPrice5 (Optional[float]): Bid price for the fifth energy-only bid level.
        EnergyOnlyBidMW6 (Optional[float]): Megawatt quantity for the sixth energy-only bid level.
        EnergyOnlyBidPrice6 (Optional[float]): Bid price for the sixth energy-only bid level.
        EnergyOnlyBidMW7 (Optional[float]): Megawatt quantity for the seventh energy-only bid level.
        EnergyOnlyBidPrice7 (Optional[float]): Bid price for the seventh energy-only bid level.
        EnergyOnlyBidMW8 (Optional[float]): Megawatt quantity for the eighth energy-only bid level.
        EnergyOnlyBidPrice8 (Optional[float]): Bid price for the eighth energy-only bid level.
        EnergyOnlyBidMW9 (Optional[float]): Megawatt quantity for the ninth energy-only bid level.
        EnergyOnlyBidPrice9 (Optional[float]): Bid price for the ninth energy-only bid level.
        EnergyOnlyBidMW10 (Optional[float]): Megawatt quantity for the tenth energy-only bid level.
        EnergyOnlyBidPrice10 (Optional[float]): Bid price for the tenth energy-only bid level.
        EnergyOnlyBidID (Optional[str]): An identifier for the energy-only bid.
        MultiHourBlockIndicator (Optional[str]): Indicator denoting a multi-hour block bid.
        BlockCurveIndicator (Optional[str]): Indicator denoting the presence of a block curve.
    Methods:
        as_tuple():
            Returns a tuple of all attribute values in the order they are defined.
    """

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

    def __post_init__(self):
        # Convert EnergyOnlyBidID to string if it's not already
        if self.EnergyOnlyBidID is not None:
            self.EnergyOnlyBidID = str(self.EnergyOnlyBidID)

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
    """
    Represents an ERCOT bid award record.
    Attributes:
        DeliveryDate (str): The delivery date associated with the bid award.
        HourEnding (int): The ending hour of the delivery period.
        SettlementPoint (str): The settlement point where the bid award applies.
        QSEName (str): The Qualified Scheduling Entity (QSE) name associated with the bid.
        EnergyOnlyBidAwardMW (float): The awarded megawatts for an energy-only bid.
        SettlementPointPrice (float): The price at the settlement point.
        BidID (str): A unique identifier for the bid.
    Methods:
        as_tuple():
            Returns:
                tuple: A tuple containing the attributes in the following order:
                       (DeliveryDate, HourEnding, SettlementPoint,
                        QSEName, EnergyOnlyBidAwardMW, SettlementPointPrice, BidID)
    """

    DeliveryDate: str
    HourEnding: int
    SettlementPoint: str
    QSEName: str
    EnergyOnlyBidAwardMW: float
    SettlementPointPrice: float
    BidID: str

    def __post_init__(self):
        # Convert BidID to string if it's not already
        self.BidID = str(self.BidID)

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
    """
    Represents an electricity offer with various energy-only offer attributes for different levels.
    Attributes:
        DeliveryDate (str): The delivery date of the offer.
        HourEnding (int): The hour ending the period for which the offer applies.
        SettlementPoint (str): The designated settlement point.
        QSEName (str): The name of the Qualified Scheduling Entity.
        EnergyOnlyOfferMW1 (Optional[float]): Megawatt quantity for energy-only offer level 1.
        EnergyOnlyOfferPrice1 (Optional[float]): Price for energy-only offer level 1.
        EnergyOnlyOfferMW2 (Optional[float]): Megawatt quantity for energy-only offer level 2.
        EnergyOnlyOfferPrice2 (Optional[float]): Price for energy-only offer level 2.
        EnergyOnlyOfferMW3 (Optional[float]): Megawatt quantity for energy-only offer level 3.
        EnergyOnlyOfferPrice3 (Optional[float]): Price for energy-only offer level 3.
        EnergyOnlyOfferMW4 (Optional[float]): Megawatt quantity for energy-only offer level 4.
        EnergyOnlyOfferPrice4 (Optional[float]): Price for energy-only offer level 4.
        EnergyOnlyOfferMW5 (Optional[float]): Megawatt quantity for energy-only offer level 5.
        EnergyOnlyOfferPrice5 (Optional[float]): Price for energy-only offer level 5.
        EnergyOnlyOfferMW6 (Optional[float]): Megawatt quantity for energy-only offer level 6.
        EnergyOnlyOfferPrice6 (Optional[float]): Price for energy-only offer level 6.
        EnergyOnlyOfferMW7 (Optional[float]): Megawatt quantity for energy-only offer level 7.
        EnergyOnlyOfferPrice7 (Optional[float]): Price for energy-only offer level 7.
        EnergyOnlyOfferMW8 (Optional[float]): Megawatt quantity for energy-only offer level 8.
        EnergyOnlyOfferPrice8 (Optional[float]): Price for energy-only offer level 8.
        EnergyOnlyOfferMW9 (Optional[float]): Megawatt quantity for energy-only offer level 9.
        EnergyOnlyOfferPrice9 (Optional[float]): Price for energy-only offer level 9.
        EnergyOnlyOfferMW10 (Optional[float]): Megawatt quantity for energy-only offer level 10.
        EnergyOnlyOfferPrice10 (Optional[float]): Price for energy-only offer level 10.
        EnergyOnlyOfferID (Optional[str]): Unique identifier for the energy-only offer.
        MultiHourBlockIndicator (Optional[str]): Indicator signaling a multi-hour block offer.
        BlockCurveIndicator (Optional[str]): Indicator for a block curve offer.
    Methods:
        as_tuple():
            Returns:
                Tuple: A tuple containing all attributes of the offer in the defined order.
    """

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

    def __post_init__(self):
        # Convert EnergyOnlyOfferID to string if it's not already
        if self.EnergyOnlyOfferID is not None:
            self.EnergyOnlyOfferID = str(self.EnergyOnlyOfferID)

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
    """
    Class representing an offer award.
    Attributes:
        DeliveryDate (str): The date of energy delivery.
        HourEnding (int): The hour (ending time) for energy delivery.
        SettlementPoint (str): The settlement location where the offer is evaluated.
        QSEName (str): The name of the qualified scheduling entity (QSE) submitting the offer.
        EnergyOnlyOfferAwardMW (float): Awarded megawatts for the energy-only component of the offer.
        SettlementPointPrice (float): The price associated with the settlement point.
        OfferID (str): A unique identifier for the offer.
    Methods:
        as_tuple():
            Returns a tuple containing the values of the attributes in the following order:
            (DeliveryDate, HourEnding, SettlementPoint, QSEName, EnergyOnlyOfferAwardMW,
             SettlementPointPrice, OfferID).
    """

    DeliveryDate: str
    HourEnding: int
    SettlementPoint: str
    QSEName: str
    EnergyOnlyOfferAwardMW: float
    SettlementPointPrice: float
    OfferID: str

    def __post_init__(self):
        # Convert OfferID to string if it's not already
        self.OfferID = str(self.OfferID)

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
