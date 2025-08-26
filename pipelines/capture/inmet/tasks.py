# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da INMET"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.inmet.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.api import get_raw_api_list
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret

# from pytz import timezone


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_temperatura_extractor(
    source: SourceTable,  # pylint: disable=W0613
    timestamp: datetime,
):
    """Cria a extração de dados da api do INMET"""

    start = timestamp - timedelta(days=1)
    data_inicio = start.strftime("%Y-%m-%d")
    data_fim = timestamp.strftime("%Y-%m-%d")

    key = get_secret(constants.INMET_SECRET_PATH.value)["key"]

    estacoes = [
        "A602",
        "A621",
        "A636",
        "A651",
        "A652",
        "A653",
        "A654",
        "A655",
        "A656",
    ]

    url_list = []
    for estacao in estacoes:
        url = f"{constants.INMET_BASE_URL.value,}/{data_inicio}/{data_fim}/{estacao}/{key}"
        url_list += url

    return partial(get_raw_api_list, url=url_list)
