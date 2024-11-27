# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Rio Ônibus"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.rioonibus.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.api import get_raw_api
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_viagem_informada_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de viagens informadas na api da Rio Ônibus"""

    extraction_day = timestamp.date() - timedelta(days=2)
    params = {
        "guidIdentificacao": get_secret(constants.RIO_ONIBUS_SECRET_PATH.value)[
            "guididentificacao"
        ],
        "datetime_processamento_inicio": extraction_day.isoformat() + "T00:00:00",
        "datetime_processamento_fim": timestamp.date().isoformat() + "T23:59:59",
    }
    return partial(
        get_raw_api,
        url=constants.VIAGEM_INFORMADA_BASE_URL.value,
        params=params,
        raw_filetype=source.raw_filetype,
    )
