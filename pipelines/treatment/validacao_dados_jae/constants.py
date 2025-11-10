# -*- coding: utf-8 -*-
"""
Valores constantes para materialização da validação dos dados da Jaé
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron, create_minute_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização da validação dos dados da Jaé
    """

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
