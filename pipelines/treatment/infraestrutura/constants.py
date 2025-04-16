# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de infraestrutura
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de infraestrutura
    """

    INFRAESTRUTURA_SELECTOR = DBTSelector(
        name="infraestrutura",
        schedule_cron=create_daily_cron(hour=4),
        initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
        incremental_delay_hours=24,
    )
