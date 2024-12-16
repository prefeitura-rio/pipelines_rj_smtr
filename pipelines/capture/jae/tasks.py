# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Jaé"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task
from pytz import timezone

from pipelines.capture.jae.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.db import get_raw_db
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_jae_general_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de tabelas da Jaé"""

    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    params = constants.JAE_TABLE_CAPTURE_PARAMS.value[source.table_id]

    start = (
        source.get_last_scheduled_timestamp(timestamp=timestamp)
        .astimezone(tz=timezone("UTC"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    end = timestamp.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    query = params["query"].format(start=start, end=end)
    database_name = params["database"]
    database = constants.JAE_DATABASE_SETTINGS.value[database_name]

    return partial(
        get_raw_db,
        query=query,
        engine=database["engine"],
        host=database["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=database_name,
    )
