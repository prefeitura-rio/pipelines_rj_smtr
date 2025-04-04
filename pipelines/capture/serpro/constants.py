# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr serpro
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_hourly_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr serpro
    """

    AUTUACAO_TABLE_ID = "autuacao"
    SERPRO_SOURCE_NAME = "serpro"

    SERPRO_PRIVATE_BUCKET_NAMES = {
        "prod": "rj-smtr-infracao-private",
        "dev": "rj-smtr-dev-infracao-private",
    }

    SERPRO_CAPTURE_PARAMS = {
        "query": """
            SELECT
                *
            FROM
                dbpro_radar_view_SMTR_VBL.tb_infracao_view
            WHERE
                PARSEDATE(SUBSTRING(auinf_dt_infracao, 1, 10), 'yyyy-MM-dd')
                BETWEEN PARSEDATE('{start_date}', 'yyyy-MM-dd')
                AND PARSEDATE('{end_date}', 'yyyy-MM-dd')
        """,
        "primary_key": ["auinf_num_auto"],
        "save_bucket_names": SERPRO_PRIVATE_BUCKET_NAMES,
        "pre_treatment_reader_args": {"dtype": "object"},
    }

    AUTUACAO_SOURCE = SourceTable(
        source_name=SERPRO_SOURCE_NAME,
        table_id=AUTUACAO_TABLE_ID,
        first_timestamp=datetime(2025, 3, 29, 0, 0, 0),
        schedule_cron=create_hourly_cron(),
        partition_date_only=True,
        primary_keys=SERPRO_CAPTURE_PARAMS["primary_key"],
        pretreatment_reader_args=SERPRO_CAPTURE_PARAMS["pre_treatment_reader_args"],
        bucket_names=SERPRO_CAPTURE_PARAMS["save_bucket_names"],
        raw_filetype="csv",
    )
