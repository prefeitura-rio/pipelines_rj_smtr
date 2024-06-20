# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_recursos
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_recursos
    """

    SUBSIDIO_SPPO_RECURSOS_TABLE_IDS = [
        {"table_id": "recursos_sppo_viagens_individuais"},
        {"table_id": "recursos_sppo_bloqueio_via"},
        {"table_id": "recursos_sppo_reprocessamento"},
    ]

    SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "dataset_id": smtr_constants.SUBSIDIO_SPPO_RECURSOS_DATASET_ID.value,
        "extract_params": {
            "token": "",
            "$select": "id,protocol,createdDate,lastUpdate",
            "$filter": "serviceFirstLevel eq '{service} - Recurso Viagens Subsídio' \
and (lastUpdate ge {start} and lastUpdate lt {end} or createdDate ge {start} \
and createdDate lt {end})",
            "$expand": "customFieldValues,customFieldValues($expand=items)",
            "$orderby": "createdDate asc",
        },
        "interval_minutes": 1440,
        "source_type": "movidesk",
        "primary_key": ["protocol"],
    }

    SUBSIDIO_SPPO_RECURSOS_MATERIALIZACAO_PARAMS = {
        "dataset_id": "br_rj_riodejaneiro_recursos",
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "datetime_recurso",
                "delay_hours": 0,
            },
            "version": {},
        },
    }
