# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Jaé
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import cron_every_day_hour_6
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da Jaé
    """

    JAE_SOURCE_NAME = "jae"

    JAE_DATABASE_SETTINGS = {
        "principal_db": {
            "engine": "mysql",
            "host": "10.5.114.227",
        },
        "tarifa_db": {
            "engine": "postgresql",
            "host": "10.5.113.254",
        },
        "transacao_db": {
            "engine": "postgresql",
            "host": "10.5.115.1",
        },
        "tracking_db": {
            "engine": "postgresql",
            "host": "10.5.15.25",
        },
        "ressarcimento_db": {
            "engine": "postgresql",
            "host": "10.5.12.50",
        },
        "gratuidade_db": {
            "engine": "postgresql",
            "host": "10.5.12.107",
        },
        "fiscalizacao_db": {
            "engine": "postgresql",
            "host": "10.5.115.29",
        },
    }

    JAE_SECRET_PATH = "smtr_jae_access_data"

    TRANSACAO_ORDEM_TABLE_ID = "transacao_ordem"

    JAE_TABLE_CAPTURE_PARAMS = {
        TRANSACAO_ORDEM_TABLE_ID: {
            "query": """
                SELECT
                    id,
                    id_ordem_ressarcimento,
                    data_processamento,
                    data_transacao
                FROM
                    transacao
                WHERE
                    DATE(data_processamento -  INTERVAL '3' HOUR) > DATE('{{ start }}')
                    AND DATE(data_processamento -  INTERVAL '3' HOUR) <= DATE('{{ end }}')
                    AND id_ordem_ressarcimento IS NOT NULL
            """,
            "database": "transacao_db",
        }
    }

    TRANSACAO_ORDEM_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=TRANSACAO_ORDEM_TABLE_ID,
        first_timestamp=datetime(2024, 11, 21, 0, 0, 0),
        schedule_cron=cron_every_day_hour_6,
        partition_date_only=True,
        max_recaptures=5,
        primary_keys=[
            "id",
            "id_ordem_ressarcimento",
            "data_processamento",
            "data_transacao",
        ],
    )
