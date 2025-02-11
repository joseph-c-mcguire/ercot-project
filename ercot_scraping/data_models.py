from dataclasses import dataclass
from typing import Optional


@dataclass
class SettlementPointPrice:
    """
    Represents settlement price details for a delivery period.
    Attributes: 
                 deliveryDate (str): The date when the delivery occurs.
        deliveryHour (int): The hour of delivery.
        deliveryInterval (int): The specific interval within the delivery hour.
        settlementPointName (str): The name of the settlement point.
        settlementPointType (str): The type or category of the settlement point.
        settlementPointPrice (float): The price at the settlement point.
        dstFlag (str): Indicator flag for Daylight Saving Time.
    Methods:
        as_tuple():
            Returns:
                tuple: A tuple containing all the settlement point price attributes in the following order:
                    (deliveryDate, deliveryHour, deliveryInterval, settlementPointName,
                     settlementPointType, settlementPointPrice, dstFlag)
    """

    deliveryDate: str
    deliveryHour: int
    deliveryInterval: int
    settlementPointName: str
    settlementPointType: str
    settlementPointPrice: float
    dstFlag: str

    def as_tuple(self):
        return (
            self.deliveryDate,
            self.deliveryHour,
            self.deliveryInterval,
            self.settlementPointName,
            self.settlementPointType,
            self.settlementPointPrice,
            self.dstFlag,
        )


@dataclass
class Bid:
    """
    Represents a bid in the ERCOT energy market.
    Attributes:
        deliveryDate (str): The delivery date of the bid.
        hourEnding (int): The hour that marks the end of the delivery period.
        settlementPoint (str): The identifier for the settlement point related to the bid.
        qseName (str): The name of the Qualified Scheduling Entity (QSE).
        energyOnlyBidMw1 (Optional[float]): Megawatt quantity for the first energy-only bid level.
        energyOnlyBidPrice1 (Optional[float]): Bid price for the first energy-only bid level.
        energyOnlyBidMW2 (Optional[float]): Megawatt quantity for the second energy-only bid level.
        energyOnlyBidPrice2 (Optional[float]): Bid price for the second energy-only bid level.
        energyOnlyBidMW3 (Optional[float]): Megawatt quantity for the third energy-only bid level.
        energyOnlyBidPrice3 (Optional[float]): Bid price for the third energy-only bid level.
        energyOnlyBidMW4 (Optional[float]): Megawatt quantity for the fourth energy-only bid level.
        energyOnlyBidPrice4 (Optional[float]): Bid price for the fourth energy-only bid level.
        energyOnlyBidMW5 (Optional[float]): Megawatt quantity for the fifth energy-only bid level.
        energyOnlyBidPrice5 (Optional[float]): Bid price for the fifth energy-only bid level.
        energyOnlyBidMW6 (Optional[float]): Megawatt quantity for the sixth energy-only bid level.
        energyOnlyBidPrice6 (Optional[float]): Bid price for the sixth energy-only bid level.
        energyOnlyBidMW7 (Optional[float]): Megawatt quantity for the seventh energy-only bid level.
        energyOnlyBidPrice7 (Optional[float]): Bid price for the seventh energy-only bid level.
        energyOnlyBidMW8 (Optional[float]): Megawatt quantity for the eighth energy-only bid level.
        energyOnlyBidPrice8 (Optional[float]): Bid price for the eighth energy-only bid level.
        energyOnlyBidMW9 (Optional[float]): Megawatt quantity for the ninth energy-only bid level.
        energyOnlyBidPrice9 (Optional[float]): Bid price for the ninth energy-only bid level.
        energyOnlyBidMW10 (Optional[float]): Megawatt quantity for the tenth energy-only bid level.
        energyOnlyBidPrice10 (Optional[float]): Bid price for the tenth energy-only bid level.
        energyOnlyBidID (Optional[str]): An identifier for the energy-only bid.
        multiHourBlockIndicator (Optional[str]): Indicator denoting a multi-hour block bid.
        blockCurve (Optional[str]): Indicator denoting the presence of a block curve.
    Methods:
        as_tuple():
            Returns a tuple of all attribute values in the order they are defined.
    """

    deliveryDate: str
    hourEnding: int
    settlementPointName: str
    qseName: str
    energyOnlyBidMw1: Optional[float] = None
    energyOnlyBidPrice1: Optional[float] = None
    energyOnlyBidMw2: Optional[float] = None
    energyOnlyBidPrice2: Optional[float] = None
    energyOnlyBidMw3: Optional[float] = None
    energyOnlyBidPrice3: Optional[float] = None
    energyOnlyBidMw4: Optional[float] = None
    energyOnlyBidPrice4: Optional[float] = None
    energyOnlyBidMw5: Optional[float] = None
    energyOnlyBidPrice5: Optional[float] = None
    energyOnlyBidMw6: Optional[float] = None
    energyOnlyBidPrice6: Optional[float] = None
    energyOnlyBidMw7: Optional[float] = None
    energyOnlyBidPrice7: Optional[float] = None
    energyOnlyBidMw8: Optional[float] = None
    energyOnlyBidPrice8: Optional[float] = None
    energyOnlyBidMw9: Optional[float] = None
    energyOnlyBidPrice9: Optional[float] = None
    energyOnlyBidMw10: Optional[float] = None
    energyOnlyBidPrice10: Optional[float] = None
    bidId: Optional[str] = None
    multiHourBlock: Optional[str] = None
    blockCurve: Optional[str] = None

    def __post_init__(self):
        # Convert energyOnlyBidID to string if it's not already
        if self.bidId is not None:
            self.bidId = str(self.bidId)

    def as_tuple(self):
        return (
            self.deliveryDate,
            self.hourEnding,
            self.settlementPointName,
            self.qseName,
            self.energyOnlyBidMw1,
            self.energyOnlyBidPrice1,
            self.energyOnlyBidMw2,
            self.energyOnlyBidPrice2,
            self.energyOnlyBidMw3,
            self.energyOnlyBidPrice3,
            self.energyOnlyBidMw4,
            self.energyOnlyBidPrice4,
            self.energyOnlyBidMw5,
            self.energyOnlyBidPrice5,
            self.energyOnlyBidMw6,
            self.energyOnlyBidPrice6,
            self.energyOnlyBidMw7,
            self.energyOnlyBidPrice7,
            self.energyOnlyBidMw8,
            self.energyOnlyBidPrice8,
            self.energyOnlyBidMw9,
            self.energyOnlyBidPrice9,
            self.energyOnlyBidMw10,
            self.energyOnlyBidPrice10,
            self.bidId,
            self.multiHourBlock,
            self.blockCurve,
        )


@dataclass
class BidAward:
    """
    Represents an ERCOT bid award record.
    Field names match the API response format with property getters/setters 
    to maintain compatibility with existing code.
    """
    deliveryDate: str  # API uses lowercase
    hourEnding: int    # API uses lowercase
    settlementPointName: str  # API uses lowercase
    qseName: str      # API uses lowercase
    energyOnlyBidAwardInMW: float  # API uses lowercase
    settlementPointPrice: float  # API uses lowercase
    bidId: str        # API uses lowercase

    def __post_init__(self):
        # Convert bidid to string if it's not already
        self.bidId = str(self.bidId)

    def as_tuple(self):
        return (
            self.deliveryDate,
            self.hourEnding,
            self.settlementPointName,
            self.qseName,
            self.energyOnlyBidAwardInMW,
            self.settlementPointPrice,
            self.bidId,
        )


@dataclass
class Offer:
    """
    Represents an electricity offer with various energy-only offer attributes for different levels.
    Attributes:
        deliveryDate (str): The delivery date of the offer.
        hourEnding (int): The hour ending the period for which the offer applies.
        settlementPoint (str): The designated settlement point.
        qseName (str): The name of the Qualified Scheduling Entity.
        energyOnlyOfferMW1 (Optional[float]): Megawatt quantity for energy-only offer level 1.
        energyOnlyOfferPrice1 (Optional[float]): Price for energy-only offer level 1.
        energyOnlyOfferMW2 (Optional[float]): Megawatt quantity for energy-only offer level 2.
        energyOnlyOfferPrice2 (Optional[float]): Price for energy-only offer level 2.
        energyOnlyOfferMW3 (Optional[float]): Megawatt quantity for energy-only offer level 3.
        energyOnlyOfferPrice3 (Optional[float]): Price for energy-only offer level 3.
        energyOnlyOfferMW4 (Optional[float]): Megawatt quantity for energy-only offer level 4.
        energyOnlyOfferPrice4 (Optional[float]): Price for energy-only offer level 4.
        energyOnlyOfferMW5 (Optional[float]): Megawatt quantity for energy-only offer level 5.
        energyOnlyOfferPrice5 (Optional[float]): Price for energy-only offer level 5.
        energyOnlyOfferMW6 (Optional[float]): Megawatt quantity for energy-only offer level 6.
        energyOnlyOfferPrice6 (Optional[float]): Price for energy-only offer level 6.
        energyOnlyOfferMW7 (Optional[float]): Megawatt quantity for energy-only offer level 7.
        energyOnlyOfferPrice7 (Optional[float]): Price for energy-only offer level 7.
        energyOnlyOfferMW8 (Optional[float]): Megawatt quantity for energy-only offer level 8.
        energyOnlyOfferPrice8 (Optional[float]): Price for energy-only offer level 8.
        energyOnlyOfferMW9 (Optional[float]): Megawatt quantity for energy-only offer level 9.
        energyOnlyOfferPrice9 (Optional[float]): Price for energy-only offer level 9.
        energyOnlyOfferMW10 (Optional[float]): Megawatt quantity for energy-only offer level 10.
        energyOnlyOfferPrice10 (Optional[float]): Price for energy-only offer level 10.
        energyOnlyOfferID (Optional[str]): Unique identifier for the energy-only offer.
        multiHourBlockIndicator (Optional[str]): Indicator signaling a multi-hour block offer.
        blockCurveIndicator (Optional[str]): Indicator for a block curve offer.
    Methods:
        as_tuple():
            Returns:
                Tuple: A tuple containing all attributes of the offer in the defined order.
    """

    deliveryDate: str
    hourEnding: int
    settlementPointName: str
    qseName: str
    energyOnlyOfferMW1: Optional[float] = None
    energyOnlyOfferPrice1: Optional[float] = None
    energyOnlyOfferMW2: Optional[float] = None
    energyOnlyOfferPrice2: Optional[float] = None
    energyOnlyOfferMW3: Optional[float] = None
    energyOnlyOfferPrice3: Optional[float] = None
    energyOnlyOfferMW4: Optional[float] = None
    energyOnlyOfferPrice4: Optional[float] = None
    energyOnlyOfferMW5: Optional[float] = None
    energyOnlyOfferPrice5: Optional[float] = None
    energyOnlyOfferMW6: Optional[float] = None
    energyOnlyOfferPrice6: Optional[float] = None
    energyOnlyOfferMW7: Optional[float] = None
    energyOnlyOfferPrice7: Optional[float] = None
    energyOnlyOfferMW8: Optional[float] = None
    energyOnlyOfferPrice8: Optional[float] = None
    energyOnlyOfferMW9: Optional[float] = None
    energyOnlyOfferPrice9: Optional[float] = None
    energyOnlyOfferMW10: Optional[float] = None
    energyOnlyOfferPrice10: Optional[float] = None
    offerId: Optional[str] = None
    multiHourBlock: Optional[str] = None
    blockCurve: Optional[str] = None

    def __post_init__(self):
        # Convert energyOnlyOfferID to string if it's not already
        if self.offerId is not None:
            self.offerId = str(self.offerId)

    def as_tuple(self):
        return (
            self.deliveryDate,
            self.hourEnding,
            self.settlementPointName,
            self.qseName,
            self.energyOnlyOfferMW1,
            self.energyOnlyOfferPrice1,
            self.energyOnlyOfferMW2,
            self.energyOnlyOfferPrice2,
            self.energyOnlyOfferMW3,
            self.energyOnlyOfferPrice3,
            self.energyOnlyOfferMW4,
            self.energyOnlyOfferPrice4,
            self.energyOnlyOfferMW5,
            self.energyOnlyOfferPrice5,
            self.energyOnlyOfferMW6,
            self.energyOnlyOfferPrice6,
            self.energyOnlyOfferMW7,
            self.energyOnlyOfferPrice7,
            self.energyOnlyOfferMW8,
            self.energyOnlyOfferPrice8,
            self.energyOnlyOfferMW9,
            self.energyOnlyOfferPrice9,
            self.energyOnlyOfferMW10,
            self.energyOnlyOfferPrice10,
            self.offerId,
            self.multiHourBlock,
            self.blockCurve,
        )


@dataclass
class OfferAward:
    """
    Class representing an offer award.
    Attributes:
        deliveryDate (str): The date of energy delivery.
        hourEnding (int): The hour (ending time) for energy delivery.
        settlementPoint (str): The settlement location where the offer is evaluated.
        qseName (str): The name of the qualified scheduling entity (QSE) submitting the offer.
        energyOnlyOfferAwardMW (float): Awarded megawatts for the energy-only component of the offer.
        settlementPointPrice (float): The price associated with the settlement point.
        offerId (str): A unique identifier for the offer.
    Methods:
        as_tuple():
            Returns a tuple containing the values of the attributes in the following order:
            (deliveryDate, hourEnding, settlementPointName, qseName, energyOnlyOfferAwardMW,
             settlementPointPrice, offerID).
    """

    deliveryDate: str
    hourEnding: int
    settlementPointName: str
    qseName: str
    energyOnlyOfferAwardInMW: float
    settlementPointPrice: float
    offerId: str

    def __post_init__(self):
        # Convert offerID to string if it's not already
        self.offerId = str(self.offerId)

    def as_tuple(self):
        return (
            self.deliveryDate,
            self.hourEnding,
            self.settlementPointName,
            self.qseName,
            self.energyOnlyOfferAwardInMW,
            self.settlementPointPrice,
            self.offerId,
        )
