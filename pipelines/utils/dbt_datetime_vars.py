# -*- coding: utf-8 -*-
"""Módulo para criação de variáveis para execução do DBT"""

from datetime import date, datetime, timedelta
from typing import Union

import basedosdados as bd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.gcp import BQTable

# from pytz import timezone


class DateRange:
    def __init__(
        self,
        datetime_column_name: str,
        truncate_hour: bool = True,
        delay_hours: int = 0,
        first_daterange_start: datetime = None,
    ):
        self.first_daterange_start = first_daterange_start
        self.datetime_column_name = datetime_column_name
        self.truncate_hour = truncate_hour
        self.value_to_save = None
        self.delay_hours = delay_hours

    def get_last_run_from_redis(self, redis_key: str) -> Union[None, datetime]:
        pass

    def get_last_run_from_bq(self, table: BQTable) -> Union[None, datetime]:
        last_run = None
        if table.exists():
            project = constants.PROJECT_NAME.value[table.env]
            query = f"""
                SELECT
                    max({self.datetime_column_name})
                FROM
                    {project}.{table.dataset_id}.{table.table_id}
            """

            log(f"Will run query:\n{query}")
            last_run = bd.read_sql(query=query, billing_project_id=project).iloc[0][0]

        if (not isinstance(last_run, datetime)) and (isinstance(last_run, date)):
            last_run = datetime(last_run.year, last_run.month, last_run.day)

        return last_run

    def create_var(
        self,
        redis_key: str,
        table: BQTable,
        timestamp: datetime,
    ) -> dict:

        last_run = (
            self.get_last_run_from_redis(redis_key=redis_key)
            or self.get_last_run_from_bq(table=table)
            or self.first_daterange_start
        )

        if last_run is None:
            return {}

        ts_format = "%Y-%m-%dT%H:%M:%S"

        start_ts = last_run.replace(second=0, microsecond=0)
        if self.truncate_hour:
            start_ts = start_ts.replace(minute=0)

        start_ts = start_ts.strftime(ts_format)

        end_ts = timestamp - timedelta(hours=self.delay_hours)

        end_ts = end_ts.replace(second=0, microsecond=0)

        if self.truncate_hour:
            end_ts = end_ts.replace(minute=0)

        end_ts = end_ts.strftime(ts_format)

        date_range = {"date_range_start": start_ts, "date_range_end": end_ts}
        self.value_to_save = end_ts

        log(f"Got date_range as: {date_range}")
