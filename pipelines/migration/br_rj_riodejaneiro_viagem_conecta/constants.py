# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_viagem_conecta
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_viagem_conecta
    """

    VIAGEM_CAPTURE_PARAMETERS = {
        "dataset_id": smtr_constants.VIAGEM_CONECTA_RAW_DATASET_ID.value,
        "table_id": "viagem_informada_conecta",
        "partition_date_only": True,
        "extract_params": {"delay_days": 2},
        "primary_key": ["id_viagem"],
        "interval_minutes": 1440,
        "source_type": "api-json",
    }

    VIAGEM_MATERIALIZACAO_PARAMS = {
        "dataset_id": smtr_constants.VIAGEM_CONECTA_RAW_DATASET_ID.value,
        "table_id": "viagem_informada_conecta",
        "upstream": True,
        "dbt_vars": {
            "run_date": {},
            "version": {},
        },
        "source_dataset_ids": [smtr_constants.VIAGEM_CONECTA_RAW_DATASET_ID.value],
        "source_table_ids": [VIAGEM_CAPTURE_PARAMETERS["table_id"]],
        "capture_intervals_minutes": [VIAGEM_CAPTURE_PARAMETERS["interval_minutes"]],
    }
