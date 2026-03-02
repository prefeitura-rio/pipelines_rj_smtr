# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da STU"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.stu.utils import extract_stu_data
from pipelines.constants import constants as smtr_constants
from pipelines.utils.gcp.bigquery import SourceTable


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_stu_extractor(
    source: SourceTable,
    timestamp: datetime,
):
    """
    Cria a extração de dados do STU a partir do bucket Airbyte.

    Compara os dados de hoje com os de ontem para retornar apenas
    registros novos ou alterados.

    Args:
        source: Objeto SourceTable com configurações da tabela
        timestamp: Timestamp de referência da execução

    Returns:
        partial: Função parcial configurada para extração dos dados
    """
    return partial(extract_stu_data, source=source, timestamp=timestamp)
