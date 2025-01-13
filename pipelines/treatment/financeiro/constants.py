# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados financeiros
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados financeiros
    """

    FINANCEIRO_BILHETAGEM_SELECTOR = DBTSelector(
        name="financeiro_bilhetagem",
        schedule_cron=create_daily_cron(hour=5, minute=15),
        initial_datetime=datetime(2025, 12, 16, 0, 0, 0),
    )
