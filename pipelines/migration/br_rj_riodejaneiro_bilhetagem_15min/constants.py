# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_bilhetagem_15min
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_bilhetagem_15min
    """

    MATERIALIZACAO_TRANSACAO_15MIN_PARAMS = {
        "dataset_id": smtr_constants.BILHETAGEM_DATASET_ID.value + "_15min",
        "table_id": "transacao_15min",
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data",
                "delay_hours": 0,
            },
            "version": {},
        },
    }
