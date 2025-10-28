# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da CCT"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task
from pytz import timezone

from pipelines.capture.cct.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.db import get_raw_db, get_raw_db_paginated
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_cct_general_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de tabelas da CCT"""

    credentials = get_secret(constants.CCT_SECRET_PATH.value)

    params = constants.CCT_TABLE_CAPTURE_PARAMS.value[source.table_id]

    start = (
        source.get_last_scheduled_timestamp(timestamp=timestamp)
        .astimezone(tz=timezone("UTC"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    end = timestamp.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    query = params["query"].format(
        start=start,
        end=end,
    )

    general_func_arguments = {
        "query": query,
        "engine": "postgresql",
        "host": credentials["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": credentials["dbname"],
        "max_retries": 3,
    }

    if source.file_chunk_size is not None:
        return partial(
            get_raw_db_paginated, page_size=source.file_chunk_size, **general_func_arguments
        )
    return partial(get_raw_db, **general_func_arguments)
