"""
Module for the data classes to use as archetypes for the DB tables.
"""

from dataclasses import dataclass
from typing import Optional
from pydantic import BaseModel


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
                tuple: A tuple containing all the settlement point price attributes
                  in the following order:
                    (deliveryDate, deliveryHour, deliveryInterval,
                    settlementPointName, settlementPointType, settlementPointPrice,
                    dstFlag)
    """

    deliveryDate: str
    deliveryHour: int
    deliveryInterval: int
    settlementPointName: str
    settlementPointType: str
    settlementPointPrice: float
    dstFlag: str
    inserted_at: Optional[str] = None

    def as_tuple(self):
        """
        Return a tuple representation of the instance.

        The tuple contains the following attributes in order:
            - deliveryDate: The date of the delivery.
            - deliveryHour: The hour corresponding to the delivery.
            - deliveryInterval: The specific delivery interval.
            - settlementPointName: The name of the settlement point.
            - settlementPointType: The type/category of the settlement point.
            - settlementPointPrice: The price associated with the settlement point.
            - dstFlag: A flag indicating whether Daylight Saving Time is in effect.

        Returns:
            tuple: A tuple containing the instance attributes as listed above.
        """
        return (
            self.deliveryDate,
            self.deliveryHour,
            self.deliveryInterval,
            self.settlementPointName,
            self.settlementPointType,
            self.settlementPointPrice,
            self.dstFlag,
            self.inserted_at,
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
        energyOnlyBidMw1 (Optional[float]): Megawatt quantity for the first
            energy-only bid level.
        energyOnlyBidPrice1 (Optional[float]): Bid price for the first
            energy-only bid level.
        energyOnlyBidMW2 (Optional[float]): Megawatt quantity for the second
            energy-only bid level.
        energyOnlyBidPrice2 (Optional[float]): Bid price for the second
            energy-only bid level.
        energyOnlyBidMW3 (Optional[float]): Megawatt quantity for the third
            energy-only bid level.
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
    inserted_at: Optional[str] = None

    def as_tuple(self):
        """
        Return a tuple representation of the object's bid data.

        The returned tuple contains the following elements:
            0: deliveryDate         - The delivery date for the bid.
            1: hourEnding           - The ending hour for the bid period.
            2: settlementPointName  - The name of the settlement point.
            3: qseName              - The QSE (Qualified Scheduling Entity) name.
            4: energyOnlyBidMw1     - Energy-only bid megawatts for level 1.
            5: energyOnlyBidPrice1  - Energy-only bid price for level 1.
            6: energyOnlyBidMw2     - Energy-only bid megawatts for level 2.
            7: energyOnlyBidPrice2  - Energy-only bid price for level 2.
            8: energyOnlyBidMw3     - Energy-only bid megawatts for level 3.
            9: energyOnlyBidPrice3  - Energy-only bid price for level 3.
           10: energyOnlyBidMw4     - Energy-only bid megawatts for level 4.
           11: energyOnlyBidPrice4  - Energy-only bid price for level 4.
           12: energyOnlyBidMw5     - Energy-only bid megawatts for level 5.
           13: energyOnlyBidPrice5  - Energy-only bid price for level 5.
           14: energyOnlyBidMw6     - Energy-only bid megawatts for level 6.
           15: energyOnlyBidPrice6  - Energy-only bid price for level 6.
           16: energyOnlyBidMw7     - Energy-only bid megawatts for level 7.
           17: energyOnlyBidPrice7  - Energy-only bid price for level 7.
           18: energyOnlyBidMw8     - Energy-only bid megawatts for level 8.
           19: energyOnlyBidPrice8  - Energy-only bid price for level 8.
           20: energyOnlyBidMw9     - Energy-only bid megawatts for level 9.
           21: energyOnlyBidPrice9  - Energy-only bid price for level 9.
           22: energyOnlyBidMw10    - Energy-only bid megawatts for level 10.
           23: energyOnlyBidPrice10 - Energy-only bid price for level 10.
           24: bidId                - The unique identifier for the bid.
           25: multiHourBlock       - Indicator if the bid spans multiple hours.
           26: blockCurve           - Information regarding the bid's block curve.
           27: inserted_at          - Timestamp indicating when the record was inserted.

        Returns:
            tuple: A tuple containing all the bid-related attributes in the order
                specified.
        """
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
            self.inserted_at,
        )


class BidSchema(BaseModel):
    deliveryDate: str
    hourEnding: int
    settlementPointName: str
    qseName: str
    energyOnlyBidMw1: float = 0.0
    energyOnlyBidPrice1: float = 0.0
    bidId: str
    multiHourBlock: str = "N"
    blockCurve: str = "N"
    # ... add all other fields as Optional[float] = 0.0
    # For brevity, only a subset is shown here


@dataclass
class BidAward:
    """
    Represents an ERCOT bid award record.
    Field names match the API response format with property getters/setters
    to maintain compatibility with existing code.
    """

    DeliveryDate: str  # API uses lowercase
    HourEnding: int  # API uses lowercase
    SettlementPoint: str  # API uses lowercase
    QSEName: str  # API uses lowercase
    EnergyOnlyBidAwardMW: float  # API uses lowercase
    SettlementPointPrice: float  # API uses lowercase
    BidId: str  # API uses lowercase
    inserted_at: Optional[str] = None

    def as_tuple(self):
        """
        Return a tuple containing the key attributes of the instance.

        The tuple includes the following elements in order:
            1. deliveryDate: The delivery date for the energy bid.
            2. hourEnding: The hour at which the bid is ending.
            3. settlementPointName: The name of the settlement point.
            4. qseName: The name of the Qualified Scheduling Entity (QSE).
            5. energyOnlyBidAwardInMW: The awarded energy-only bid in megawatts.
            6. settlementPointPrice: The price at the settlement point.
            7. bidId: The unique identifier for the bid.
            8. inserted_at: The timestamp when the record was inserted.

        Returns:
            tuple: A tuple containing (deliveryDate, hourEnding,
            settlementPointName, qseName, energyOnlyBidAwardInMW,
            settlementPointPrice, bidId, inserted_at).
        """
        return (
            self.DeliveryDate,
            self.HourEnding,
            self.SettlementPoint,
            self.QSEName,
            self.EnergyOnlyBidAwardMW,
            self.SettlementPointPrice,
            self.BidId,
            self.inserted_at,
        )


class BidAwardSchema(BaseModel):
    deliveryDate: str
    hourEnding: int
    settlementPointName: str
    qseName: str
    energyOnlyBidAwardInMW: float
    settlementPointPrice: float
    bidId: str
    inserted_at: str = None


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
    inserted_at: Optional[str] = None

    def __post_init__(self):
        """
        Post-initialization hook that ensures the offerId attribute is stored as a string.

        If offerId is not None, converts it to a string. This simplifies subsequent operations
        that work with offerId by guaranteeing a consistent data type.
        """
        # Convert offerId to string if it's not already
        if self.offerId is not None:
            self.offerId = str(self.offerId)

    def as_tuple(self):
        """
        Return a tuple representation of the object's attributes.

        The tuple contains the following elements in order:
            1. deliveryDate: The delivery date.
            2. hourEnding: The hour ending.
            3. settlementPointName: The name of the settlement point.
            4. qseName: The QSE name.
            5. energyOnlyOfferMW1: Energy only offer in MW for first block.
            6. energyOnlyOfferPrice1: Energy only offer price for first block.
            7. energyOnlyOfferMW2: Energy only offer in MW for second block.
            8. energyOnlyOfferPrice2: Energy only offer price for second block.
            9. energyOnlyOfferMW3: Energy only offer in MW for third block.
            10. energyOnlyOfferPrice3: Energy only offer price for third block.
            11. energyOnlyOfferMW4: Energy only offer in MW for fourth block.
            12. energyOnlyOfferPrice4: Energy only offer price for fourth block.
            13. energyOnlyOfferMW5: Energy only offer in MW for fifth block.
            14. energyOnlyOfferPrice5: Energy only offer price for fifth block.
            15. energyOnlyOfferMW6: Energy only offer in MW for sixth block.
            16. energyOnlyOfferPrice6: Energy only offer price for sixth block.
            17. energyOnlyOfferMW7: Energy only offer in MW for seventh block.
            18. energyOnlyOfferPrice7: Energy only offer price for seventh block.
            19. energyOnlyOfferMW8: Energy only offer in MW for eighth block.
            20. energyOnlyOfferPrice8: Energy only offer price for eighth block.
            21. energyOnlyOfferMW9: Energy only offer in MW for ninth block.
            22. energyOnlyOfferPrice9: Energy only offer price for ninth block.
            23. energyOnlyOfferMW10: Energy only offer in MW for tenth block.
            24. energyOnlyOfferPrice10: Energy only offer price for tenth block.
            25. offerId: The identifier for the offer.
            26. multiHourBlock: Boolean or indicator for multi-hour block offers.
            27. blockCurve: The block curve data associated with the offer.
            28. inserted_at: The timestamp when the record was inserted.

        Returns:
            tuple: A tuple containing the object's attribute values in the specified order.
        """
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
            self.inserted_at,
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
        energyOnlyOfferAwardMW (float): Awarded megawatts for the energy-only
            component of the offer.
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
    inserted_at: Optional[str] = None

    def as_tuple(self):
        """
        Return a tuple containing the object's core data attributes.

        The returned tuple includes, in order:
            1. deliveryDate: The delivery date.
            2. hourEnding: The hour ending indicator.
            3. settlementPointName: The name of the settlement point.
            4. qseName: The name of the QSE.
            5. energyOnlyOfferAwardInMW: The awarded energy-only offer in MW.
            6. settlementPointPrice: The price at the settlement point.
            7. offerId: The identifier for the offer.
            8. inserted_at: The timestamp when the record was inserted.

        Returns:
            tuple: A tuple of the aforementioned attributes.
        """
        return (
            self.deliveryDate,
            self.hourEnding,
            self.settlementPointName,
            self.qseName,
            self.energyOnlyOfferAwardInMW,
            self.settlementPointPrice,
            self.offerId,
            self.inserted_at,
        )
