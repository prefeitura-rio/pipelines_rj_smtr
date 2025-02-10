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
            "check_gps_treatment__gps_sppo": {
                "description": "Todos os dados de GPS foram devidamente tratados"
            },
            "dbt_utils.unique_combination_of_columns__gps_sppo": {
                "description": "Todos os registros são únicos"
            },
            "not_null__timestamp_gps__gps_sppo": {
                "description": "Todos os registros possuem timestamp_gps não nulo"
            },
            "not_null__id_veiculo__gps_sppo": {
                "description": "Todos os registros possuem id_veiculo não nulo"
            },
            "not_null__servico__gps_sppo": {
                "description": "Todos os registros possuem servico não nulo"
            },
            "not_null__latitude__gps_sppo": {
                "description": "Todos os registros possuem latitude não nula"
            },
            "not_null__longitude__gps_sppo": {
                "description": "Todos os registros possuem longitude não nula"
            },
            "not_null__status__gps_sppo": {
                "description": "Todos os registros possuem servico não nulo"
            },
        }
    }
