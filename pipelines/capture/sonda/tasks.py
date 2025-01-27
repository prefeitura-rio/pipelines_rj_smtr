# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da SONDA"""
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
import requests
from prefect import task

from pipelines.capture.sonda.constants import constants
from pipelines.capture.templates.utils import DefaultSourceTable
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.api import get_raw_api_params_list
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_viagem_informada_extractor(
    source: DefaultSourceTable,  # pylint: disable=W0613
    timestamp: datetime,
):
    """Cria a extração de viagens informadas na api da SONDA"""

    end_date = timestamp.date()
    start_date = end_date - timedelta(days=2)

    loging_response = requests.post(
        constants.VIAGEM_INFORMADA_LOGIN_URL.value,
        data=get_secret(constants.SONDA_SECRET_PATH.value),
        timeout=120,
    )

    loging_response.raise_for_status()

    key = loging_response.json()["IdentificacaoLogin"]

    params = [
        {
            "datetime_processamento": d.strftime("%d/%m/%Y 00:00:00"),
        }
        for d in pd.date_range(start_date, end_date)
    ]

    headers = {"Authorization": key}

    return partial(
        get_raw_api_params_list,
        url=constants.VIAGEM_INFORMADA_BASE_URL.value,
        params_list=params,
        headers=headers,
    )
