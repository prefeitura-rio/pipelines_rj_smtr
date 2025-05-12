# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da ZIRIX"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.zirix.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.api import get_raw_api
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_gps_extractor(
    source: SourceTable,  # pylint: disable=W0613
    timestamp: datetime,
):
    """Cria a extração de dados de GPS na api da ZIRIX"""

    if source.table_id == constants.ZIRIX_REGISTROS_TABLE_ID.value:
        url = f"{constants.ZIRIX_BASE_URL.value}/EnvioIplan?"
        date_range = {
            "date_range_start": (timestamp - timedelta(minutes=6)).strftime("%Y-%m-%d %H:%M:%S"),
            "date_range_end": (timestamp - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
        }
    else:
        url = f"{constants.ZIRIX_BASE_URL.value}/EnvioViagensRetroativas?"
        date_range = {
            "date_range_start": (timestamp - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
            "date_range_end": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        }

    headers = get_secret(constants.ZIRIX_SECRET_PATH.value)
    key = list(headers)[0]
    url = f"{url}guidIdentificacao={headers[key]}"
    url += f"&dataInicial={date_range['date_range_start']}&dataFinal={date_range['date_range_end']}"

    return partial(get_raw_api, url=url)
