# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de fiscalização de veiculos"""
from datetime import timedelta
from functools import partial

from prefect import task

from pipelines.capture.veiculo_fiscalizacao.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.gdrive import get_google_sheet_xlsx


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_veiculo_fiscalizacao_lacre_extractor():
    """Cria a extração da planilha de controle de lacre dos veículos"""

    return partial(
        get_google_sheet_xlsx,
        spread_sheet_id=constants.VEICULO_LACRE_SHEET_ID.value,
        sheet_name=constants.VEICULO_LACRE_SHEET_NAME.value,
    )
