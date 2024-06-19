# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_onibus_gps_zirix
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_onibus_gps_zirix
    """

    ZIRIX_API_SECRET_PATH = "zirix_api"
    ZIRIX_BASE_URL = "https://integration.systemsatx.com.br/Globalbus/SMTR"

    GPS_SPPO_ZIRIX_RAW_DATASET_ID = "br_rj_riodejaneiro_onibus_gps_zirix"
    GPS_SPPO_ZIRIX_TREATED_TABLE_ID = "gps_sppo_zirix"
