# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Rio Ônibus"""
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
from prefect import task

from pipelines.capture.rioonibus.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.api import get_raw_api_params_list
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_viagem_informada_extractor(
    source: SourceTable,  # pylint: disable=W0613
    timestamp: datetime,
):
    """Cria a extração de viagens informadas na api da Rio Ônibus"""

    end_date = timestamp.date()
    start_date = end_date - timedelta(days=2)
    api_key = get_secret(constants.RIO_ONIBUS_SECRET_PATH.value)["guididentificacao"]
    params = [
        {
            "guidIdentificacao": api_key,
            "datetime_processamento_inicio": d.date().isoformat() + "T00:00:00",
            "datetime_processamento_fim": d.date().isoformat() + "T23:59:59",
        }
        for d in pd.date_range(start_date, end_date)
    ]

    return partial(
        get_raw_api_params_list,
        url=constants.VIAGEM_INFORMADA_BASE_URL.value,
        params_list=params,
    )
