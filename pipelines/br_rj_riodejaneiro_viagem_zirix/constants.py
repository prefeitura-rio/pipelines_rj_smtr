# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_viagem_zirix
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_viagem_zirix
    """

    VIAGEM_ZIRIX_RAW_DATASET_ID = "br_rj_riodejaneiro_viagem_zirix"

    VIAGEM_CAPTURE_PARAMETERS = {
        "dataset_id": VIAGEM_ZIRIX_RAW_DATASET_ID,
        "table_id": "viagem_informada",
        "partition_date_only": False,
        "extract_params": {"delay_minutes": 5},
        "primary_key": ["id_viagem"],
        "interval_minutes": 10,
        "source_type": "api-json",
    }
