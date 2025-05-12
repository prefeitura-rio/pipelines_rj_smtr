# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da CONECTA
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_minute_cron
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.pretreatment import pretreat_gps_realocacao, pretreat_gps_registros


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da CONECTA
    """

    CONECTA_SOURCE_NAME = "conecta"
    CONECTA_SECRET_PATH = "conecta_api"
    CONECTA_BASE_URL = "https://ccomobility.com.br/webservices/binder/wsconecta"
    CONECTA_CAPTURE_DELAY = 60

    CONECTA_REGISTROS_ENDPOINT = "envioSMTR"
    CONECTA_REGISTROS_TABLE_ID = "registros"
    CONECTA_REGISTROS_SOURCE = SourceTable(
        source_name=CONECTA_SOURCE_NAME,
        table_id=CONECTA_REGISTROS_TABLE_ID,
        first_timestamp=datetime(2025, 5, 9, 0, 0, 0),
        schedule_cron=create_minute_cron(),
        primary_keys=["id_veiculo", "datetime_servidor"],
        pretreat_funcs=[pretreat_gps_registros],
    )

    CONECTA_REALOCACAO_ENDPOINT = "EnvioRealocacoesSMTR"
    CONECTA_REALOCACAO_TABLE_ID = "realocacao"
    CONECTA_REALOCACAO_SOURCE = SourceTable(
        source_name=CONECTA_SOURCE_NAME,
        table_id=CONECTA_REALOCACAO_TABLE_ID,
        first_timestamp=datetime(2025, 5, 9, 0, 0, 0),
        schedule_cron=create_minute_cron(minute=10),
        primary_keys=["id_veiculo", "datetime_processamento"],
        pretreat_funcs=[pretreat_gps_realocacao],
    )
