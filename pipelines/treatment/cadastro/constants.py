# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de cadastro
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron, create_hourly_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de cadastro
    """

    CADASTRO_SELECTOR = DBTSelector(
        name="cadastro",
        schedule_cron=create_hourly_cron(minute=10),
        initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
    )

    CADASTRO_VEICULO_SELECTOR = DBTSelector(
        name="cadastro_veiculo",
        schedule_cron=create_daily_cron(hour=7),
        initial_datetime=datetime(2025, 6, 23, 0, 0, 0),
    )
