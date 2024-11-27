# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da SONDA"""
from datetime import datetime, timedelta
from functools import partial

import requests
from prefect import task

from pipelines.capture.sonda.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.api import get_raw_api
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_viagem_informada_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de viagens informadas na api da SONDA"""

    extraction_day = timestamp.date() - timedelta(days=2)

    loging_response = requests.post(
        constants.VIAGEM_INFORMADA_LOGIN_URL.value,
        data=get_secret(constants.SONDA_SECRET_PATH.value),
        timeout=120,
    )

    loging_response.raise_for_status()

    key = loging_response.json()["IdentificacaoLogin"]

    params = {
        "datetime_processamento": extraction_day.strftime("%d/%m/%Y 00:00:00"),
    }

    headers = {"Authorization": key}

    return partial(
        get_raw_api,
        url=constants.VIAGEM_INFORMADA_BASE_URL.value,
        params=params,
        headers=headers,
        raw_filetype=source.raw_filetype,
    )
