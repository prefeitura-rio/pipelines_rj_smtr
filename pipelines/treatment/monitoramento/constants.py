# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de monitoramento
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import (
    create_daily_cron,
    create_hourly_cron,
    create_minute_cron,
)
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de monitoramento
    """

    VIAGEM_INFORMADA_SELECTOR = DBTSelector(
        name="viagem_informada",
        schedule_cron=create_daily_cron(hour=7, minute=30),
        initial_datetime=datetime(2024, 10, 16, 0, 0, 0),
    )

    VIAGEM_VALIDACAO_SELECTOR = DBTSelector(
        name="viagem_validacao",
        schedule_cron=create_daily_cron(hour=8),
        initial_datetime=datetime(2024, 10, 12, 0, 0, 0),
        incremental_delay_hours=48,
    )

    GPS_SELECTOR = DBTSelector(
        name="gps",
        schedule_cron=create_hourly_cron(minute=6),
        initial_datetime=datetime(2025, 5, 27, 0, 0, 0),
        incremental_delay_hours=1,
    )

    GPS_PRE_CHECKS_LIST = {
        "staging_realocacao": {
            "check_gps_capture__staging_realocacao": {
                "description": "Todos os dados de realocação foram capturados"
            }
        },
        "staging_gps": {
            "check_gps_capture__staging_gps": {
                "description": "Todos os dados de GPS foram capturados"
            }
        },
        "gps_sppo": {
            "check_gps_treatment__gps_sppo": {
                "description": "Todos os dados de GPS foram devidamente tratados"
            },
            "dbt_utils.unique_combination_of_columns__gps_sppo": {
                "description": "Todos os registros são únicos"
            },
        },
    }

    GPS_POST_CHECKS_LIST = {
        "gps": {
            "check_gps_treatment__gps": {
                "description": "Todos os dados de GPS foram devidamente tratados"
            },
            "dbt_utils.unique_combination_of_columns__gps": {
                "description": "Todos os registros são únicos"
            },
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
    }

    GPS_PRE_TESTS = {
        "test_name": "check_gps_capture__staging_gps check_gps_capture__staging_realocacao",
        "checks_list": GPS_PRE_CHECKS_LIST,
    }

    GPS_POST_TESTS = {
        "model": "gps",
        "checks_list": GPS_POST_CHECKS_LIST,
    }

    GPS_15_MINUTOS_SELECTOR = DBTSelector(
        name="gps_15_minutos",
        schedule_cron=create_minute_cron(minute=15),
        initial_datetime=datetime(2025, 5, 27, 0, 0, 0),
    )

    MONITORAMENTO_VEICULO_SELECTOR = DBTSelector(
        name="monitoramento_veiculo",
        schedule_cron=create_daily_cron(hour=7),
        initial_datetime=datetime(2025, 5, 28, 0, 0, 0),
    )
