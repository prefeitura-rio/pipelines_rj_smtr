# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de bilhetagem
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de bilhetagem
    """

    TRANSACAO_ORDEM_SELECTOR = DBTSelector(
        name="transacao_ordem",
        schedule_cron=create_daily_cron(hour=6, minute=10),
        initial_datetime=datetime(2024, 11, 21, 0, 0, 0),
    )

    TRANSACAO_VALOR_ORDEM_SELECTOR = DBTSelector(
        name="transacao_valor_ordem",
        schedule_cron=create_daily_cron(hour=7, minute=40),
        initial_datetime=datetime(2025, 2, 4, 0, 0, 0),
    )
