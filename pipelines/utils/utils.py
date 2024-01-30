# -*- coding: utf-8 -*-
"""General purpose functions"""
import io
from datetime import date, datetime
from typing import Any

import pandas as pd
import pytz

from pipelines.constants import constants


def custom_serialization(obj: Any) -> Any:
    """
    Function to serialize not JSON serializable objects

    Args:
        obj (Any): Object to serialize

    Returns:
        Any: Serialized object
    """
    if isinstance(obj, (pd.Timestamp, date)):
        if isinstance(obj, pd.Timestamp):
            if obj.tzinfo is None:
                obj = obj.tz_localize("UTC").tz_convert(constants.TIMEZONE.value)
        return obj.isoformat()

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def data_info_str(data: pd.DataFrame):
    """
    Return dataframe info as a str to log

    Args:
        data (pd.DataFrame): dataframe

    Returns:
        data.info() as a string
    """
    buffer = io.StringIO()
    data.info(buf=buffer)
    return buffer.getvalue()


def create_timestamp_captura(timestamp: datetime) -> str:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=pytz.UTC)

    return timestamp.astimezone(tz=pytz.timezone(constants.TIMEZONE.value)).strftime(
        "%Y-%m-%d %H:%M:%S-03:00"
    )


def isostr_to_datetime(datetime_str: str) -> datetime:
    converted = datetime.fromisoformat(datetime_str)
    if converted.tzinfo is None:
        converted = converted.replace(tzinfo=pytz.UTC)
    else:
        converted = converted.astimezone(tz=pytz.timezone("UTC"))

    return converted
