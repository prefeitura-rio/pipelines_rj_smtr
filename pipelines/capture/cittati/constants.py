# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da CITTATI
"""

from datetime import datetime
from enum import Enum

from pipelines.capture.cittati.utils import (
    pretreat_cittati_realocacao,
    pretreat_cittati_registros,
)
from pipelines.schedules import create_minute_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da CITTATI
    """

    CITTATI_SOURCE_NAME = "cittati"
    CITTATI_SECRET_PATH = "cittati_api"
    CITTATI_BASE_URL = "https://servicos.cittati.com.br/WSIntegracaoCittati/SMTR/v2"
    CITTATI_CAPTURE_DELAY = 60

    CITTATI_REGISTROS_TABLE_ID = "registros"
    CITTATI_REGISTROS_SOURCE = SourceTable(
        source_name=CITTATI_SOURCE_NAME,
        table_id=CITTATI_REGISTROS_TABLE_ID,
        first_timestamp=datetime(2025, 5, 9, 0, 0, 0),
        schedule_cron=create_minute_cron(),
        primary_keys=["id_veiculo", "datetime"],
        pretreat_funcs=[pretreat_cittati_registros],
    )

    CITTATI_REALOCACAO_TABLE_ID = "realocacao"
    CITTATI_REALOCACAO_SOURCE = SourceTable(
        source_name=CITTATI_SOURCE_NAME,
        table_id=CITTATI_REALOCACAO_TABLE_ID,
        first_timestamp=datetime(2025, 5, 9, 0, 0, 0),
        schedule_cron=create_minute_cron(minute=10),
        primary_keys=["id_veiculo", "datetime_processamento"],
        pretreat_funcs=[pretreat_cittati_realocacao],
    )
