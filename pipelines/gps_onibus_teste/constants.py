# -*- coding: utf-8 -*-
"""
Valores constantes para pipelines gps_onibus_teste
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para pipelines gps_onibus_teste
    """

    # GPS SPPO #
    GPS_SPPO_API_BASE_URL = "https://integration.systemsatx.com.br/Globalbus/SMTR/EnvioIplan?"

    GPS_SPPO_API_SECRET_PATH = "gps_onibus_api_zirix"

    GPS_SPPO_RAW_DATASET_ID = "gps_onibus_teste"
    GPS_SPPO_RAW_TABLE_ID = "registros"
    GPS_SPPO_CAPTURE_DELAY_V1 = 1
    GPS_SPPO_CAPTURE_DELAY_V2 = 60
    GPS_SPPO_RECAPTURE_DELAY_V2 = 6
    GPS_SPPO_MATERIALIZE_DELAY_HOURS = 1

    # REALOCAÇÃO #
    GPS_SPPO_REALOCACAO_RAW_TABLE_ID = "realocacao"
