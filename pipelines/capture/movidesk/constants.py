# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Rio Ônibus
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados do MoviDesk
    """

    MOVIDESK_SOURCE_NAME = "movidesk"
    MOVIDESK_SECRET_PATH = "sppo_subsidio_recursos_api"  # movidesk_api
    TICKETS_BASE_URL = "https://api.movidesk.com/public/v1/tickets"
    TICKETS_TABLE_ID = "tickets"
    TICKETS_SOURCE = SourceTable(
        source_name=MOVIDESK_SOURCE_NAME,
        table_id=TICKETS_TABLE_ID,
        first_timestamp=datetime(2024, 12, 1, 0, 0, 0),
        schedule_cron=create_daily_cron(hour=0),
        partition_date_only=True,
        # max_recaptures=5,
        primary_keys=["protocol"],
    )
