# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da ZIRIX"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.zirix.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.gps import create_generic_gps_extractor
from pipelines.utils.gcp.bigquery import SourceTable


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_gps_extractor(
    source: SourceTable,  # pylint: disable=W0613
    timestamp: datetime,
):
    """Cria a extração de dados de GPS na api da ZIRIX"""

    return partial(
        create_generic_gps_extractor,
        source=source,
        timestamp=timestamp,
        base_url=constants.ZIRIX_BASE_URL.value,
        registros_endpoint=constants.ZIRIX_REGISTROS_ENDPOINT.value,
        realocacao_endpoint=constants.ZIRIX_REALOCACAO_ENDPOINT.value,
        secret_path=constants.ZIRIX_SECRET_PATH.value,
        registros_table_id=constants.ZIRIX_REGISTROS_TABLE_ID.value,
    )
