# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados do STU
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.utils.gcp.bigquery import SourceTable

STU_SOURCE_NAME = "stu"
STU_PRIVATE_BUCKET_NAMES = {"prod": "rj-smtr-dev-airbyte", "dev": "rj-smtr-dev-airbyte"}


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados do STU
    """

    STU_VISTORIA_TABLE_ID = "vistoria"
    STU_PF_TABLE_ID = "pessoa_fisica"
    STU_PJ_TABLE_ID = "pessoa_juridica"
    STU_PERMISSAO_TABLE_ID = "permissao"

    STU_TABLE_CAPTURE_PARAMS = {
        STU_VISTORIA_TABLE_ID: {
            "primary_keys": ["id_vistoria", "anoexe"],
            "first_timestamp": datetime(2025, 10, 3, 0, 0, 0),
        },
        # STU_PF_TABLE_ID: {
        #     "primary_keys": ["ratr"],
        #     "first_timestamp": datetime(2025, 10, 6, 0, 0, 0),
        # },
        # STU_PJ_TABLE_ID: {
        #     "primary_keys": ["cgc"],
        #     "first_timestamp": datetime(2025, 10, 6, 0, 0, 0),
        # },
        # STU_PERMISSAO_TABLE_ID: {
        #     "primary_keys": ["tptran", "tpperm", "termo", "dv"],
        #     "first_timestamp": datetime(2025, 10, 6, 0, 0, 0),
        # },
    }

    STU_SOURCES = [
        SourceTable(
            source_name=STU_SOURCE_NAME,
            table_id=k,
            first_timestamp=v.get("first_timestamp"),
            schedule_cron=create_daily_cron(hour=8),
            primary_keys=v["primary_keys"],
            pretreatment_reader_args=v.get("pretreatment_reader_args"),
            pretreat_funcs=v.get("pretreat_funcs"),
            bucket_names=STU_PRIVATE_BUCKET_NAMES,
            partition_date_only=True,
            max_recaptures=v.get("max_recaptures", 2),
            raw_filetype=v.get("raw_filetype", "csv"),
        )
        for k, v in STU_TABLE_CAPTURE_PARAMS.items()
    ]
