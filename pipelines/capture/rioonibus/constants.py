# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Rio Ônibus
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import cron_every_day_hour_7
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da Rio Ônibus
    """

    RIO_ONIBUS_SOURCE_NAME = "rioonibus"
    RIO_ONIBUS_SECRET_PATH = "rioonibus_api"
    VIAGEM_INFORMADA_BASE_URL = "https://us-east1-papo-tec.cloudfunctions.net/viagem_informada_smtr"
    VIAGEM_INFORMADA_TABLE_ID = "viagem_informada"
    VIAGEM_INFORMADA_SOURCE = SourceTable(
        source_name=RIO_ONIBUS_SOURCE_NAME,
        table_id="viagem_informada",
        first_timestamp=datetime(2024, 9, 21, 0, 0, 0),
        schedule_cron=cron_every_day_hour_7,
        partition_date_only=True,
        max_recaptures=5,
        primary_keys=["id_viagem"],
    )
