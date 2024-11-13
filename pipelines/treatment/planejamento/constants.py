# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de planejamento
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import cron_every_day_hour_1
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de planejamento
    """

    PLANEJAMENTO_DIARIO_SELECTOR = DBTSelector(
        name="planejamento_diario",
        schedule_cron=cron_every_day_hour_1,
        initial_datetime=datetime(2024, 9, 1, 0, 0, 0),
    )
