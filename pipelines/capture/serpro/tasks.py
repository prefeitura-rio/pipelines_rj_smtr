# -*- coding: utf-8 -*-
from datetime import datetime
from functools import partial
from typing import Union

from prefect import task

from pipelines.capture.serpro.utils import extract_serpro_data
from pipelines.utils.gcp.bigquery import SourceTable


@task
def create_serpro_extractor(
    source: SourceTable, timestamp: Union[str, datetime]  # pylint: disable=W0613
):
    """
    Cria uma função para extrair dados do SERPRO

    Args:
        source (SourceTable): Objeto contendo informações da tabela
        timestamp (datetime): Timestamp da execução

    Returns:
        Callable: Função para extração dos dados
    """

    return partial(extract_serpro_data, timestamp=timestamp)
