# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_onibus_gps
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_onibus_gps
    """

    GPS_DATA_CHECKS_LIST = {
        "gps_sppo": {
            "unique_columns_gps_sppo": {"description": "Todos os registros são únicos"},
            "not_null_timestamp_gps": {
                "description": "Todos os registros possuem timestamp_gps não nulo"
            },
            "not_null_id_veiculo": {
                "description": "Todos os registros possuem id_veiculo não nulo"
            },
            "not_null_servico": {"description": "Todos os registros possuem servico não nulo"},
            "not_null_latitude": {"description": "Todos os registros possuem latitude não nula"},
            "not_null_longitude": {"description": "Todos os registros possuem longitude não nula"},
            "not_null_status": {"description": "Todos os registros possuem servico não nulo"},
        }
    }
