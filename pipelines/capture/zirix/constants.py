# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da ZIRIX
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_minute_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da ZIRIX
    """

    ZIRIX_SOURCE_NAME = "zirix"
    ZIRIX_SECRET_PATH = "zirix_api"
    ZIRIX_BASE_URL = "https://integration.systemsatx.com.br/Globalbus/SMTR/V2"
    ZIRIX_CAPTURE_DELAY = 60

    ZIRIX_REGISTROS_ENDPOINT = "EnvioIplan"
    ZIRIX_REGISTROS_TABLE_ID = "registros"
    ZIRIX_REGISTROS_SOURCE = SourceTable(
        source_name=ZIRIX_SOURCE_NAME,
        table_id=ZIRIX_REGISTROS_TABLE_ID,
        first_timestamp=datetime(2025, 5, 9, 0, 0, 0),
        schedule_cron=create_minute_cron(),
        primary_keys=["id_veiculo", "datetime_servidor"],
    )

    ZIRIX_REALOCACAO_ENDPOINT = "EnvioViagensRetroativas"
    ZIRIX_REALOCACAO_TABLE_ID = "realocacao"
    ZIRIX_REALOCACAO_SOURCE = SourceTable(
        source_name=ZIRIX_SOURCE_NAME,
        table_id=ZIRIX_REALOCACAO_TABLE_ID,
        first_timestamp=datetime(2025, 5, 9, 0, 0, 0),
        schedule_cron=create_minute_cron(minute=10),
        primary_keys=["id_veiculo", "datetime_processamento"],
    )
