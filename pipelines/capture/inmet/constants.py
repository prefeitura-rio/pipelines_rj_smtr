# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados do INMET
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados do INMET
    """

    INMET_SOURCE_NAME = "inmet"
    INMET_SECRET_PATH = "inmet_api"
    INMET_BASE_URL = "https://apitempo.inmet.gov.br/token/estacao"

    INMET_METEOROLOGIA_TABLE_ID = "meteorologia"
    INMET_METEOROLOGIA_SOURCE = SourceTable(
        source_name=INMET_SOURCE_NAME,
        table_id=INMET_METEOROLOGIA_TABLE_ID,
        first_timestamp=datetime(2025, 7, 16, 0, 0, 0),
        schedule_cron=create_daily_cron(hour=5),
        partition_date_only=True,
        primary_keys=["CD_ESTACAO", "DT_MEDICAO", "HR_MEDICAO"],
    )
