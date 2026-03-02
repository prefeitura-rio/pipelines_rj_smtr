# -*- coding: utf-8 -*-
"""
Valores constantes para materialização da validação dos dados da Jaé
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron, create_minute_cron
from pipelines.treatment.templates.utils import DBTSelector, DBTTest


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização da validação dos dados da Jaé
    """

    VALIDACAO_DADOS_JAE_POST_CHECKS_LIST = {
        "integracao_nao_realizada": {
            "unique": {"description": "Todos os registros são únicos"},
        },
    }

    VALIDACAO_DADOS_JAE_DAILY_TEST = DBTTest(
        model="integracao_nao_realizada",
        checks_list=VALIDACAO_DADOS_JAE_POST_CHECKS_LIST,
        truncate_date=True,
        delay_days_start=1,
    )

    VALIDACAO_DADOS_JAE_SELECTOR = DBTSelector(
        name="validacao_dados_jae",
        schedule_cron=create_daily_cron(hour=12),
        initial_datetime=datetime(2024, 12, 30, 0, 0, 0),
    )

    ALERTA_TRANSACAO_SELECTOR = DBTSelector(
        name="alerta_transacao",
        schedule_cron=create_minute_cron(minute=10),
        initial_datetime=datetime(2025, 10, 13, 10, 0, 0),
    )
