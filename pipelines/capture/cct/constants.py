# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da CCT
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_hourly_cron
from pipelines.utils.gcp.bigquery import SourceTable

CCT_SOURCE_NAME = "cct"


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da CCT
    """

    CCT_SECRET_PATH = "cct_db"
    CCT_PRIVATE_BUCKET_NAMES = {"prod": "rj-smtr-jae-private", "dev": "rj-smtr-dev-private"}

    CCT_TABLE_CAPTURE_PARAMS = {
        "ordem_pagamento": {
            "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento
                WHERE
                    createdAt BETWEEN '{start}'
                    AND '{end}'
                    OR updatedAt BETWEEN '{start}'
                    AND '{end}'
            """,
            "primary_keys": ["id"],
            "capture_flow": "auxiliar",
        },
    }

    JAE_AUXILIAR_SOURCES = [
        SourceTable(
            source_name=CCT_SOURCE_NAME,
            table_id=k,
            first_timestamp=v.get("first_timestamp", datetime(2024, 1, 7, 0, 0, 0)),
            schedule_cron=create_hourly_cron(),
            primary_keys=v["primary_keys"],
            pretreatment_reader_args=v.get("pre_treatment_reader_args"),
            pretreat_funcs=v.get("pretreat_funcs"),
            bucket_names=v.get("save_bucket_names"),
            partition_date_only=v.get("partition_date_only", True),
            max_recaptures=v.get("max_recaptures", 60),
            raw_filetype=v.get("raw_filetype", "json"),
            file_chunk_size=v.get("file_chunk_size"),
        )
        for k, v in CCT_TABLE_CAPTURE_PARAMS.items()
        if v.get("capture_flow") == "auxiliar"
    ]
