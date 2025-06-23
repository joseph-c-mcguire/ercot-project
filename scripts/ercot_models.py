"""
ERCOT Data Models using Pydantic for validation and normalization
"""
import re
from typing import Optional, Union
from datetime import datetime
from dataclasses import dataclass
from pydantic import (
    BaseModel, Field, field_validator, model_validator, ValidationError
)
import pandas as pd
import requests


def normalize_header(header: str) -> str:
    """Normalize a single header by removing special characters."""
    return re.sub(r'[^A-Za-z0-9]', '', str(header)).upper()


def normalize_headers(headers):
    """Normalize a list or pandas Index of headers"""
    return [normalize_header(h) for h in headers]


def normalize_dict_keys(d):
    """Normalize all keys in a dictionary"""
    return {normalize_header(k): v for k, v in d.items()}


class NormalizedBaseModel(BaseModel):
    """Base model with automatic key normalization"""

    @model_validator(mode="before")
    @classmethod
    def normalize_keys(cls, values):
        if isinstance(values, dict):
            values = normalize_dict_keys(values)

            # Handle common field mappings
            if ('SETTLEMENTPOINT' in values and
                    'SETTLEMENTPOINTNAME' not in values):
                values['SETTLEMENTPOINTNAME'] = values.pop('SETTLEMENTPOINT')

            # Handle various ID field variations
            bid_id_variations = ['ENERGYONLYBIDID', 'BIDID']
            offer_id_variations = ['ENERGYONLYOFFERID', 'OFFERID']

            for bid_var in bid_id_variations:
                if bid_var in values and 'BIDID' not in values:
                    values['BIDID'] = values.pop(bid_var)
                    break

            for offer_var in offer_id_variations:
                if offer_var in values and 'OFFERID' not in values:
                    values['OFFERID'] = values.pop(offer_var)
                    break

            # Convert empty strings to None for numeric fields
            for key, value in list(values.items()):
                if isinstance(value, str) and value.strip() == '':
                    patterns = ['MW', 'PRICE', 'AWARD']
                    if any(pattern in key for pattern in patterns):
                        values[key] = None

        return values

    class Config:
        validate_assignment = True
        use_enum_values = True
        arbitrary_types_allowed = True


class DAMEnergyBidAward(NormalizedBaseModel):
    """Model for DAM Energy Bid Awards"""
    DELIVERYDATE: str = Field(description="Delivery date in YYYY-MM-DD format")
    HOURENDING: int = Field(description="Hour ending (1-24)")
    SETTLEMENTPOINTNAME: str = Field(description="Settlement point name")
    QSENAME: str = Field(description="QSE name")
    ENERGYONLYBIDAWARDINMW: Optional[float] = Field(
        default=None, description="Energy only bid award in MW"
    )
    SETTLEMENTPOINTPRICE: Optional[float] = Field(
        default=None, description="Settlement point price"
    )
    BIDID: str = Field(description="Bid ID")

    @field_validator('HOURENDING')
    @classmethod
    def validate_hour_ending(cls, v):
        if not 1 <= v <= 24:
            raise ValueError(f'Hour ending must be between 1 and 24, got {v}')
        return v

    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion"""
        return {
            'DELIVERYDATE': self.DELIVERYDATE,
            'HOURENDING': self.HOURENDING,
            'SETTLEMENTPOINTNAME': self.SETTLEMENTPOINTNAME,
            'QSENAME': self.QSENAME,
            'ENERGYONLYBIDAWARDINMW': self.ENERGYONLYBIDAWARDINMW,
            'SETTLEMENTPOINTPRICE': self.SETTLEMENTPOINTPRICE,
            'BIDID': self.BIDID
        }


class DAMEnergyBid(NormalizedBaseModel):
    """Model for DAM Energy Bids - WIDE FORMAT (no segment expansion)"""
    DELIVERYDATE: str
    HOURENDING: int
    SETTLEMENTPOINTNAME: str
    QSENAME: str
    BIDID: Union[str, int, float]
    MULTIHOURBLOCK: Optional[bool] = None
    BLOCKCURVE: Optional[bool] = None
    ENERGYONLYBIDMW1: Optional[float] = None
    ENERGYONLYBIDPRICE1: Optional[float] = None
    ENERGYONLYBIDMW2: Optional[float] = None
    ENERGYONLYBIDPRICE2: Optional[float] = None
    ENERGYONLYBIDMW3: Optional[float] = None
    ENERGYONLYBIDPRICE3: Optional[float] = None
    ENERGYONLYBIDMW4: Optional[float] = None
    ENERGYONLYBIDPRICE4: Optional[float] = None
    ENERGYONLYBIDMW5: Optional[float] = None
    ENERGYONLYBIDPRICE5: Optional[float] = None
    ENERGYONLYBIDMW6: Optional[float] = None
    ENERGYONLYBIDPRICE6: Optional[float] = None
    ENERGYONLYBIDMW7: Optional[float] = None
    ENERGYONLYBIDPRICE7: Optional[float] = None
    ENERGYONLYBIDMW8: Optional[float] = None
    ENERGYONLYBIDPRICE8: Optional[float] = None
    ENERGYONLYBIDMW9: Optional[float] = None
    ENERGYONLYBIDPRICE9: Optional[float] = None
    ENERGYONLYBIDMW10: Optional[float] = None
    ENERGYONLYBIDPRICE10: Optional[float] = None

    @field_validator('MULTIHOURBLOCK', 'BLOCKCURVE', mode='before')
    @classmethod
    def parse_boolean(cls, v):
        if v is None or pd.isna(v):
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            v = v.strip().upper()
            if v in ("Y", "YES", "TRUE", "T", "1", "V"):
                return True
            if v in ("N", "NO", "FALSE", "F", "0", ""):
                return False
        return None

    @field_validator('BIDID', mode='before')
    @classmethod
    def convert_bid_id(cls, v):
        return str(v) if v is not None else None


class DAMEnergyOfferAward(NormalizedBaseModel):
    """Model for DAM Energy Offer Awards"""
    DELIVERYDATE: str
    HOURENDING: int
    SETTLEMENTPOINTNAME: str
    QSENAME: str
    ENERGYONLYOFFERAWARDINMW: Optional[float] = None
    SETTLEMENTPOINTPRICE: Optional[float] = None
    OFFERID: str

    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion"""
        return {
            'DELIVERYDATE': self.DELIVERYDATE,
            'HOURENDING': self.HOURENDING,
            'SETTLEMENTPOINTNAME': self.SETTLEMENTPOINTNAME,
            'QSENAME': self.QSENAME,
            'ENERGYONLYOFFERAWARDINMW': self.ENERGYONLYOFFERAWARDINMW,
            'SETTLEMENTPOINTPRICE': self.SETTLEMENTPOINTPRICE,
            'OFFERID': self.OFFERID
        }


class DAMEnergyOffer(NormalizedBaseModel):
    """Model for DAM Energy Offers - WIDE FORMAT (no segment expansion)"""
    DELIVERYDATE: str
    HOURENDING: int
    SETTLEMENTPOINTNAME: str
    QSENAME: str
    OFFERID: Union[str, int, float]
    MULTIHOURBLOCK: Optional[bool] = None
    BLOCKCURVE: Optional[bool] = None
    ENERGYONLYOFFERMW1: Optional[float] = None
    ENERGYONLYOFFERPRICE1: Optional[float] = None
    ENERGYONLYOFFERMW2: Optional[float] = None
    ENERGYONLYOFFERPRICE2: Optional[float] = None
    ENERGYONLYOFFERMW3: Optional[float] = None
    ENERGYONLYOFFERPRICE3: Optional[float] = None
    ENERGYONLYOFFERMW4: Optional[float] = None
    ENERGYONLYOFFERPRICE4: Optional[float] = None
    ENERGYONLYOFFERMW5: Optional[float] = None
    ENERGYONLYOFFERPRICE5: Optional[float] = None
    ENERGYONLYOFFERMW6: Optional[float] = None
    ENERGYONLYOFFERPRICE6: Optional[float] = None
    ENERGYONLYOFFERMW7: Optional[float] = None
    ENERGYONLYOFFERPRICE7: Optional[float] = None
    ENERGYONLYOFFERMW8: Optional[float] = None
    ENERGYONLYOFFERPRICE8: Optional[float] = None
    ENERGYONLYOFFERMW9: Optional[float] = None
    ENERGYONLYOFFERPRICE9: Optional[float] = None
    ENERGYONLYOFFERMW10: Optional[float] = None
    ENERGYONLYOFFERPRICE10: Optional[float] = None

    @field_validator('MULTIHOURBLOCK', 'BLOCKCURVE', mode='before')
    @classmethod
    def parse_boolean(cls, v):
        if v is None or pd.isna(v):
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            v = v.strip().upper()
            if v in ("Y", "YES", "TRUE", "T", "1", "V"):
                return True
            if v in ("N", "NO", "FALSE", "F", "0", ""):
                return False
        return None

    @field_validator('OFFERID', mode='before')
    @classmethod
    def convert_offer_id(cls, v):
        return str(v) if v is not None else None


class SPPData(NormalizedBaseModel):
    """Model for Settlement Point Price data"""
    DELIVERYDATE: str
    DELIVERYHOUR: int
    DELIVERYINTERVAL: Optional[int] = None
    SETTLEMENTPOINTNAME: str
    SETTLEMENTPOINTTYPE: str
    SETTLEMENTPOINTPRICE: Optional[float] = None
    DSTFLAG: Optional[str] = None

    @field_validator('DELIVERYHOUR')
    @classmethod
    def validate_delivery_hour(cls, v):
        if not 0 <= v <= 23:
            msg = f'Delivery hour must be between 0 and 23, got {v}'
            raise ValueError(msg)
        return v

    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion"""
        return {
            'DELIVERYDATE': self.DELIVERYDATE,
            'DELIVERYHOUR': self.DELIVERYHOUR,
            'SETTLEMENTPOINT': self.SETTLEMENTPOINTNAME,
            'SETTLEMENTPOINTTYPE': self.SETTLEMENTPOINTTYPE,
            'AVGSETTLEMENTPOINTPRICE': self.SETTLEMENTPOINTPRICE,
            'DSTFLAG': self.DSTFLAG
        }


class ERCOTTrackingQSE(NormalizedBaseModel):
    """Model for tracked QSE information"""
    NAME: str
    SHORTNAME: str
    DUNSNUMBER: Optional[float] = None
    MARKETPARTICIPANTTYPE: Optional[str] = None
    REFDATE: Optional[str] = None
    WEB01: Optional[str] = None
    WEB02: Optional[str] = None
    AWFLAG02: Optional[int] = None
    ACTVPWRGRP: Optional[int] = None

    @field_validator('WEB01', 'WEB02', mode='before')
    @classmethod
    def nan_to_none(cls, v):
        # Accept None, string, or NaN (from pandas)
        if v is None:
            return None
        if isinstance(v, float) and pd.isna(v):
            return None
        if isinstance(v, str) and v.strip().lower() == "nan":
            return None
        return str(v)


class BatchProcessor:
    """Utility class for batch processing with Pydantic models (wide-table only)"""

    @staticmethod
    def process_dataframe_with_model(df: pd.DataFrame,
                                     model_class: type[BaseModel],
                                     tracked_qses: Optional[set] = None
                                     ) -> pd.DataFrame:
        """Process DataFrame rows through Pydantic model for validation"""

        # Normalize column names
        df.columns = normalize_headers(df.columns)

        # Filter by tracked QSEs if provided
        if tracked_qses and 'QSENAME' in df.columns:
            df = df[df['QSENAME'].str.upper().isin(tracked_qses)]

        # Validate rows and collect valid data
        valid_rows = []
        for _, row in df.iterrows():
            try:
                model_instance = model_class(**row.to_dict())
                valid_rows.append(model_instance.model_dump())
            except (ValidationError, TypeError, ValueError):
                # Log validation errors if needed
                continue

        return pd.DataFrame(valid_rows)


@dataclass
class AuthToken:
    """Authentication token with expiration time"""
    access_token: str
    expires_at: datetime


class ERCOTOpenApiClient:
    """
    Simple ERCOT OpenAPI client for archive and bundle endpoints.
    Now supports Bearer token, extra headers, rate limiting, and auto-retry.
    """
    BASE_URL = "https://api.ercot.com/api/public-reports"

    def __init__(self, subscription_key: str, bearer_token: str = None,
                 extra_headers: dict = None, auth_callback=None):
        self.subscription_key = subscription_key
        self.bearer_token = bearer_token
        self.auth_callback = auth_callback  # Function to refresh bearer token
        self.session = requests.Session()
        headers = {
            "Ocp-Apim-Subscription-Key": self.subscription_key
        }
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        if extra_headers:
            headers.update(extra_headers)
        self.session.headers.update(headers)

    def _make_request_with_retry(self, method, url, max_retries=3, **kwargs):
        """Make request with 429/401 error handling and retries"""
        import time

        for attempt in range(max_retries):
            try:
                resp = getattr(self.session, method.lower())(url, **kwargs)

                # Handle 401 - try to refresh token once
                if resp.status_code == 401 and self.auth_callback and attempt == 0:
                    new_token = self.auth_callback()
                    if new_token:
                        self.bearer_token = new_token
                        self.session.headers["Authorization"] = f"Bearer {new_token}"
                        continue  # Retry with new token

                # Handle 429 - rate limiting
                if resp.status_code == 429:
                    wait_time = 2 if attempt == 0 else 60
                    time.sleep(wait_time)
                    continue

                # If still 401 or 404, raise specific errors
                if resp.status_code == 401:
                    raise PermissionError(
                        "Missing or invalid subscription key or bearer token."
                    )
                if resp.status_code == 404:
                    raise FileNotFoundError(f"Resource not found: {url}")

                resp.raise_for_status()
                return resp

            except requests.exceptions.RequestException:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

        raise Exception(f"Failed after {max_retries} attempts")

    def get_archive_metadata(self, emil_id: str):
        url = f"{self.BASE_URL}/archive/{emil_id}"
        resp = self._make_request_with_retry('GET', url)
        return resp.json()

    def download_archive(self, emil_id: str, out_path: str, download_id: Optional[str] = None, is_bundle: bool = False):
        """
        Download archive or bundle. If is_bundle is True, require download_id and use bundle endpoint.
        If not, use the legacy /archive/{emil_id}/download endpoint (POST) or /archive/{emil_id}?download=... (GET).
        """
        if is_bundle:
            if not download_id:
                raise ValueError(
                    "download_id (docId) is required for bundle downloads.")
            url = f"{self.BASE_URL}/bundle/{emil_id}?download={download_id}"
            resp = self._make_request_with_retry('GET', url)
        else:
            if download_id:
                url = f"{self.BASE_URL}/archive/{emil_id}?download={download_id}"
                resp = self._make_request_with_retry('GET', url)
            else:
                url = f"{self.BASE_URL}/archive/{emil_id}/download"
                resp = self._make_request_with_retry('POST', url)

        with open(out_path, "wb") as f:
            f.write(resp.content)
        return out_path

    def get_bundle_metadata(self, emil_id: str):
        url = f"{self.BASE_URL}/bundle/{emil_id}"
        resp = self._make_request_with_retry('GET', url)
        return resp.json()

    def download_bundle(self, emil_id: str, download_id: str, out_path: str):
        url = f"{self.BASE_URL}/bundle/{emil_id}?download={download_id}"
        resp = self._make_request_with_retry('GET', url)
        with open(out_path, "wb") as f:
            f.write(resp.content)
        return out_path
