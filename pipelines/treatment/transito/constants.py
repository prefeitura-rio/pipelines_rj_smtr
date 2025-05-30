# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de trânsito
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de trânsito
    """

    TRANSITO_AUTUACAO_SELECTOR = DBTSelector(
        name="transito_autuacao",
        schedule_cron=create_daily_cron(hour=7, minute=40),
        initial_datetime=datetime(2025, 3, 29, 0, 0, 0),
    )

    SNAPSHOT_TRANSITO_SELECTOR = DBTSelector(
        name="snapshot_transito",
    )
