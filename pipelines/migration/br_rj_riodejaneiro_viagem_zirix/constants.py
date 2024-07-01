# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_viagem_zirix
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_viagem_zirix
    """

    VIAGEM_CAPTURE_PARAMETERS = {
        "dataset_id": smtr_constants.VIAGEM_ZIRIX_RAW_DATASET_ID.value,
        "table_id": "viagem_informada",
        "partition_date_only": False,
        "extract_params": {"delay_minutes": 5},
        "primary_key": ["id_viagem"],
        "interval_minutes": 10,
        "source_type": "api-json",
    }

    VIAGEM_MATERIALIZACAO_PARAMS = {
        "dataset_id": smtr_constants.VIAGEM_ZIRIX_RAW_DATASET_ID.value,
        "table_id": "viagem_informada",
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data",
                "delay_hours": 1,
            },
            "version": {},
        },
        "source_dataset_ids": [smtr_constants.VIAGEM_ZIRIX_RAW_DATASET_ID.value],
        "source_table_ids": [VIAGEM_CAPTURE_PARAMETERS["table_id"]],
        "capture_intervals_minutes": [VIAGEM_CAPTURE_PARAMETERS["interval_minutes"]],
    }
