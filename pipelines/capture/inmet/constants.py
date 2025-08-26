# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da INMET
"""

from datetime import datetime
from enum import Enum

from pipelines.capture.inmet.utils import pretreatment_inmet
from pipelines.schedules import create_hourly_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da INMET
    """

    INMET_SOURCE_NAME = "inmet"
    INMET_SECRET_PATH = "inmet_api"
    INMET_BASE_URL = "https://apitempo.inmet.gov.br/token/estacao"

    INMET_TEMPERATURA_TABLE_ID = "temperatura"
    INMET_TEMPERATURA_SOURCE = SourceTable(
        source_name=INMET_SOURCE_NAME,
        table_id=INMET_TEMPERATURA_TABLE_ID,
        first_timestamp=datetime(2025, 7, 16, 0, 0, 0),
        schedule_cron=create_hourly_cron(),
        primary_keys=["id_estacao"],
        pretreat_funcs=[pretreatment_inmet],
    )
