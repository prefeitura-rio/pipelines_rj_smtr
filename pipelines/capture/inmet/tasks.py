# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da INMET"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.inmet.constants import constants
from pipelines.capture.inmet.utils import get_inmet_estacoes
from pipelines.constants import constants as smtr_constants
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
    """Cria a extração de dados de TEMPERATURA na api do INMET"""

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

    start = timestamp - timedelta(days=1)
    data_inicio = start.strftime("%Y-%m-%d")
    data_fim = timestamp.strftime("%Y-%m-%d")

    return partial(
        get_inmet_estacoes,
        base_url=constants.INMET_BASE_URL.value,
        data_inicio=data_inicio,
        data_fim=data_fim,
        estacoes=estacoes,
        token=key,
    )
