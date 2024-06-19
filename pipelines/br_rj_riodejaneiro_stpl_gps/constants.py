# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_stpl_gps
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_stpl_gps
    """

    GPS_STPL_API_BASE_URL = "http://zn4.m2mcontrol.com.br/api/integracao/veiculos"
    GPS_STPL_API_SECRET_PATH = "stpl_api"

    GPS_STPL_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_STPL_RAW_DATASET_ID = "br_rj_riodejaneiro_stpl_gps"
    GPS_STPL_RAW_TABLE_ID = "registros"
    GPS_STPL_TREATED_TABLE_ID = "gps_stpl"
