# -*- coding: utf-8 -*-
"""Module to get incremental capture values"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone

from pipelines.constants import constants
from pipelines.utils.fs import read_raw_data
from pipelines.utils.gcp import BQTable
from pipelines.utils.utils import isostr_to_datetime


@dataclass
class IncrementalData:
    start_value: Any
    end_value: Any
    execution_mode: str

    def __getitem__(self, key):
        return self.__dict__[key]


class IncrementalStrategy(ABC):
    """
    Base class for Incremental strategies
    To create a new Incremental strategy:
    1. inherit this class
    2. implement the abstract methods:
        to_dict: must return a dict with one key (the strategy name)
        and the value as a dict with the concrete class init args
        _get_start_value: must return the incremental start value
        _get_end_value: must return the incremental end value
        _get_value_to_save: must return the value to save on redis at the flow end
    3. overwrite the class attributes:
        _save_parser: the function to convert the value to save on redis
        _redis_value_default: the default value when redis has no value
        _redis_value_parser: the function to convert the value that came from redis
    """

    _save_parser: Callable[[Any], Any] = str
    _redis_value_default = None
    _redis_value_parser: Callable[[Any], Any] = str

    def __init__(
        self,
        max_incremental_window: Any,
        first_value: Any = None,
        incremental_reference_column: str = None,
    ) -> None:
        self._max_incremental_window = max_incremental_window
        self._incremental_reference_column = incremental_reference_column
        self._first_value = first_value
        self._execution_mode = None
        self._force_full = None
        self._redis_key = None
        self._start_value = None
        self._end_value = None
        self._initialized = False

    def __getitem__(self, key):
        return self.__dict__[key]

    def initialize(
        self,
        table: BQTable,
        force_full: bool = False,
        overwrite_start_value: Any = None,
        overwrite_end_value: Any = None,
    ):
        self._redis_key = f"{table.env}.{table.dataset_id}.{table.table_id}"

        self._force_full = force_full

        last_redis_value = self._query_redis().get(constants.REDIS_LAST_CAPTURED_VALUE_KEY.value)

        self._execution_mode = "full" if last_redis_value is None or force_full else "incr"

        last_redis_value = last_redis_value or self._first_value

        last_redis_value = (
            last_redis_value
            if last_redis_value is None
            else self._redis_value_parser(last_redis_value)
        )

        self._start_value = (
            self._redis_value_parser(overwrite_start_value)
            if overwrite_start_value is not None
            else self._get_start_value(last_redis_value=last_redis_value)
        )

        self._end_value = (
            self._redis_value_parser(overwrite_end_value)
            if overwrite_end_value is not None
            else self._get_end_value(start_value=self._start_value)
        )

        assert self._start_value <= self._end_value, "start_value greater than end_value"

        self._initialized = True

    @property
    def incremental_info(self):
        if not self._initialized:
            return AttributeError("You must initilize the strategy, run the method: initialize")

        return IncrementalData(
            start_value=self._start_value,
            end_value=self._end_value,
            execution_mode=self._execution_mode,
        )

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @abstractmethod
    def _get_start_value(self, last_redis_value: Any) -> Any:
        pass

    @abstractmethod
    def _get_end_value(self, start_value: Any) -> Any:
        pass

    @abstractmethod
    def get_value_to_save(self, raw_filepath: str) -> Any:
        pass

    def _query_redis(self) -> dict:
        redis_client = get_redis_client()
        content = redis_client.get(self._redis_key)
        if content is None:
            content = {}
        return content

    def save_on_redis(
        self,
        value_to_save: Any,
    ) -> str:
        content = self._query_redis()
        old_val = content.get(constants.REDIS_LAST_CAPTURED_VALUE_KEY.value)

        old_val = (
            self._redis_value_default if old_val is None else self._redis_value_parser(old_val)
        )

        if value_to_save > old_val:
            redis_client = get_redis_client()
            content[constants.REDIS_LAST_CAPTURED_VALUE_KEY.value] = self._save_parser(
                value_to_save
            )
            redis_client.set(self._redis_key, content)
            return "Value saved on Redis!"

        return "Last captured value is lower than value that is on Redis, will not overwrite it"


class IDIncremental(IncrementalStrategy):
    _save_parser: Callable[[Any], Any] = int
    _redis_value_default = 0
    _redis_value_parser: Callable[[Any], Any] = int

    def __init__(
        self,
        max_incremental_window: int,
        incremental_reference_column: str,
        first_value: Any = None,
    ) -> None:
        super().__init__(
            max_incremental_window=max_incremental_window,
            first_value=first_value,
            incremental_reference_column=incremental_reference_column,
        )

    def to_dict(self) -> dict:
        return {
            "id": {
                "max_incremental_window": self._max_incremental_window,
                "first_value": self._first_value,
                "incremental_reference_column": self._incremental_reference_column,
            }
        }

    def _get_start_value(self, last_redis_value: int) -> int:
        return last_redis_value

    def _get_end_value(self, start_value: int) -> int:
        return start_value + int(self._max_incremental_window)

    def get_value_to_save(self, raw_filepath: str) -> int:
        df = read_raw_data(filepath=raw_filepath)
        return (
            df[self._incremental_reference_column]
            .dropna()
            .astype(str)
            .str.replace(".0", "")
            .astype(int)
            .max()
        )


class DatetimeIncremental(IncrementalStrategy):
    _save_parser: Callable[[Any], Any] = datetime.isoformat
    _redis_value_default = datetime.min
    _redis_value_parser: Callable[[Any], Any] = isostr_to_datetime

    def __init__(
        self,
        max_incremental_window: dict,
        first_value: str = None,
        incremental_reference_column: str = None,
        reference_column_ts_format: str = "iso",
        reference_column_tz: str = "UTC",
    ) -> None:
        super().__init__(
            max_incremental_window=max_incremental_window,
            first_value=first_value,
            incremental_reference_column=incremental_reference_column,
        )
        self._reference_column_ts_format = reference_column_ts_format
        self._reference_column_tz = reference_column_tz
        self._timestamp = None

    def initialize(
        self,
        table: BQTable,
        force_full: bool = False,
        overwrite_start_value: Any = None,
        overwrite_end_value: Any = None,
    ):
        self._timestamp = table.timestamp
        return super().initialize(
            table=table,
            force_full=force_full,
            overwrite_start_value=overwrite_start_value,
            overwrite_end_value=overwrite_end_value,
        )

    def to_dict(self) -> dict:
        return {
            "datetime": {
                "max_incremental_window": self._max_incremental_window,
                "first_value": self._first_value,
                "incremental_reference_column": self._incremental_reference_column,
                "reference_column_ts_format": self._reference_column_ts_format,
            }
        }

    def _get_start_value(self, last_redis_value: datetime) -> datetime:
        return last_redis_value

    def _get_end_value(self, start_value: datetime) -> datetime:
        return min(self._timestamp, start_value + timedelta(**self._max_incremental_window))

    def get_value_to_save(self, raw_filepath: str) -> str:
        if self._incremental_reference_column is None:
            return self._end_value

        df = read_raw_data(filepath=raw_filepath)
        dates = df[self._incremental_reference_column].dropna().astype(str)
        match self._reference_column_ts_format:
            case "iso":
                dates = pd.to_datetime(dates)
            case "unix-ms":
                dates = pd.to_datetime(dates, unit="ms")
            case "unix-s":
                dates = pd.to_datetime(dates, unit="s")
            case _:
                dates = pd.to_datetime(dates, format=self._reference_column_ts_format)

        if dates.dt.tz is None:
            dates.dt.tz_localize(timezone(self._reference_column_tz))
        return dates.dt.tz_convert(timezone(constants.TIMEZONE.value)).max().isoformat()


def incremental_strategy_from_dict(strategy_dict: dict) -> IncrementalStrategy:
    incremental_type = list(strategy_dict.keys())[0]
    class_map = {
        "id": IDIncremental,
        "datetime": DatetimeIncremental,
    }
    return class_map[incremental_type](**strategy_dict[incremental_type])
