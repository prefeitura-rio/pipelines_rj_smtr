# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de bilhetagem
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron, create_hourly_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de bilhetagem
    """

    TRANSACAO_SELECTOR = DBTSelector(
        name="transacao",
        schedule_cron=create_hourly_cron(),
        initial_datetime=datetime(2025, 1, 16, 0, 0, 0),
        incremental_delay_hours=1,
    )

    PASSAGEIRO_HORA_SELECTOR = DBTSelector(
        name="passageiro_hora",
        schedule_cron=create_hourly_cron(minute=10),
        initial_datetime=datetime(2025, 1, 16, 0, 0, 0),
    )

    GPS_VALIDADOR_SELECTOR = DBTSelector(
        name="gps_validador",
        schedule_cron=create_hourly_cron(),
        initial_datetime=datetime(2025, 1, 16, 0, 0, 0),
        incremental_delay_hours=1,
    )

    INTEGRACAO_SELECTOR = DBTSelector(
        name="integracao",
        schedule_cron=create_daily_cron(hour=5, minute=15),
        initial_datetime=datetime(2025, 1, 16, 0, 0, 0),
    )

    TRANSACAO_ORDEM_SELECTOR = DBTSelector(
        name="transacao_ordem",
        schedule_cron=create_daily_cron(hour=6, minute=10),
        initial_datetime=datetime(2024, 11, 21, 0, 0, 0),
    )
