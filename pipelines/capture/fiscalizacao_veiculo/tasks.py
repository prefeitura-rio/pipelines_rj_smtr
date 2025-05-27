# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de fiscalização de veiculos"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.fiscalizacao_veiculo.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.gdrive import get_google_sheet_xlsx
from pipelines.utils.gcp.bigquery import SourceTable


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_veiculo_lacre_extractor(
    source: SourceTable,
    timestamp: datetime,
):
    """Cria a extração da planilha de controle de lacre dos veículos"""
    filter_expr = None

    if source.exists():

        last_capture = source.get_last_scheduled_timestamp(timestamp=timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        filter_expr = (
            f"ultima_atualizacao > {last_capture} and ultima_atualizacao <= {timestamp_str}"
        )

    return partial(
        get_google_sheet_xlsx,
        spread_sheet_id=constants.VEICULO_LACRE_SHEET_ID.value,
        sheet_name=constants.VEICULO_LACRE_SHEET_NAME.value,
        filter_expr=filter_expr,
    )
