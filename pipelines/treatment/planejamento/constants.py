# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de planejamento
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de planejamento
    """

    PLANEJAMENTO_DIARIO_SELECTOR = DBTSelector(
        name="planejamento_diario",
        schedule_cron=create_daily_cron(hour=1),
        initial_datetime=datetime(2024, 9, 1, 0, 0, 0),
    )
