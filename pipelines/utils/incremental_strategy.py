# -*- coding: utf-8 -*-
"""Module to get incremental capture values"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone

from pipelines.constants import constants
from pipelines.utils.fs import read_raw_data
from pipelines.utils.gcp import BQTable
from pipelines.utils.utils import isostr_to_datetime


@dataclass
class IncrementalInfo:
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

    def __init__(
        self,
        max_incremental_window: Any,
        first_value: Any = None,
        incremental_reference_column: str = None,
    ) -> None:
        self._max_incremental_window = max_incremental_window
        self._incremental_reference_column = incremental_reference_column
        self._first_value = first_value
        self._force_full = None
        self._redis_key = None
        self.incremental_info = None

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

        last_redis_value = self.query_redis().get(constants.REDIS_LAST_CAPTURED_VALUE_KEY.value)
        log(f"last_redis_value = {last_redis_value}")

        execution_mode = "full" if last_redis_value is None or force_full else "incr"

        last_redis_value = self.parse_redis_value(last_redis_value) or self._first_value

        log(f"overwrite_start_value = {overwrite_start_value}")
        log(f"overwrite_end_value = {overwrite_end_value}")

        start_value = (
            self.parse_redis_value(overwrite_start_value)
            if overwrite_start_value is not None
            else self.parse_redis_value(last_redis_value)
        )

        end_value = (
            self.parse_redis_value(overwrite_end_value)
            if overwrite_end_value is not None
            else self._get_end_value(start_value=start_value)
        )
        log(f"teste = {self._get_end_value(start_value=start_value)}")
        log(f"start_value = {overwrite_start_value}")
        log(f"end_value = {end_value}")

        if start_value is not None:
            assert start_value <= end_value, "start_value greater than end_value"

        self.incremental_info = IncrementalInfo(
            start_value=start_value,
            end_value=end_value,
            execution_mode=execution_mode,
        )

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @abstractmethod
    def _get_end_value(self, start_value: Any) -> Any:
        pass

    @abstractmethod
    def get_value_to_save(self, raw_filepath: str) -> Any:
        pass

    def query_redis(self) -> dict:
        redis_client = get_redis_client()
        content = redis_client.get(self._redis_key)
        if content is None:
            content = {}
        return content

    @abstractmethod
    def parse_redis_value(self, value: Any) -> Any:
        pass

    @abstractmethod
    def parse_value_to_save(self, value: Any) -> Any:
        pass

    def save_on_redis(
        self,
        value_to_save: Any,
    ) -> str:
        content = self.query_redis()
        old_value = content.get(constants.REDIS_LAST_CAPTURED_VALUE_KEY.value)

        if old_value is not None:
            old_value = self.parse_redis_value(old_value)
            value_to_save = min(old_value, value_to_save)

        redis_client = get_redis_client()
        content[constants.REDIS_LAST_CAPTURED_VALUE_KEY.value] = self.parse_value_to_save(
            value_to_save
        )
        redis_client.set(self._redis_key, content)
        return f"Value {value_to_save} saved on Redis!"


class IDIncremental(IncrementalStrategy):
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

    def _get_end_value(self, start_value: int) -> int:
        if start_value is not None:
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

    def parse_redis_value(self, value: Any) -> Any:
        if value is not None:
            return int(value)

    def parse_value_to_save(self, value: Any) -> Any:
        return int(value)


class DatetimeIncremental(IncrementalStrategy):
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
        overwrite_start_value: str = None,
        overwrite_end_value: str = None,
    ):
        self._timestamp = table.timestamp
        log(f"timestamp = {self._timestamp}")
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

    def _get_end_value(self, start_value: datetime) -> datetime:
        log("chegou _get_end_value")
        if start_value is not None:
            end_value = min(
                self._timestamp, start_value + timedelta(**self._max_incremental_window)
            )
        else:
            log("chegou _get_end_value else")
            end_value = self._timestamp

        if not end_value.tzinfo:
            end_value = end_value.replace(tzinfo=timezone("UTC"))
        else:
            end_value = end_value.astimezone(tz=timezone("UTC"))

    def get_value_to_save(self, raw_filepath: str) -> str:
        if self._incremental_reference_column is None:
            return self.incremental_info.end_value.isoformat()

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
        return dates.dt.tz_convert(timezone("UTC")).max().isoformat()

    def parse_redis_value(self, value: str) -> datetime:
        if value is not None:
            log(f"rodou parse_redis_value valor: {value}")
            return isostr_to_datetime(value)

    def parse_value_to_save(self, value: datetime) -> str:
        return value.isoformat()


def incremental_strategy_from_dict(strategy_dict: dict) -> IncrementalStrategy:
    incremental_type = list(strategy_dict.keys())[0]
    class_map = {
        "id": IDIncremental,
        "datetime": DatetimeIncremental,
    }
    return class_map[incremental_type](**strategy_dict[incremental_type])
