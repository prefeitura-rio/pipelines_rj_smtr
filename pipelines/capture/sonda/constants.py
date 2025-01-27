# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da SONDA
"""

from datetime import datetime
from enum import Enum

from pipelines.capture.templates.utils import DefaultSourceTable
from pipelines.schedules import create_daily_cron


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da SONDA
    """

    SONDA_SOURCE_NAME = "sonda"
    SONDA_SECRET_PATH = "sonda_api"
    VIAGEM_INFORMADA_LOGIN_URL = "http://consultaviagem.m2mfrota.com.br/AutenticarUsuario"
    VIAGEM_INFORMADA_BASE_URL = "https://zn4.sinopticoplus.com/servico-dados/api/v1/obterDadosGTFS"
    VIAGEM_INFORMADA_TABLE_ID = "viagem_informada"
    VIAGEM_INFORMADA_SOURCE = DefaultSourceTable(
        source_name=SONDA_SOURCE_NAME,
        table_id=VIAGEM_INFORMADA_TABLE_ID,
        first_timestamp=datetime(2024, 9, 10, 0, 0, 0),
        schedule_cron=create_daily_cron(hour=7, minute=10),
        partition_date_only=True,
        max_recaptures=5,
        primary_keys=["id_viagem"],
    )
