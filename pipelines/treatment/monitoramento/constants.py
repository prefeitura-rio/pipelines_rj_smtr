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
from pipelines.treatment.templates.utils import DBTSelector, DBTTest


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

    GPS_DAILY_TEST = DBTTest(
        model="gps",
        checks_list=GPS_POST_CHECKS_LIST,
        delay_days=1,
        truncate_date=True,
    )

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

    MONITORAMENTO_VEICULO_CHECKS_LIST = {
        "veiculo_fiscalizacao_lacre": {
            "dbt_utils.relationships_where__id_auto_infracao__veiculo_fiscalizacao_lacre": {
                "description": "Todos os ids de auto infração estão na tabela de autuação"
            },
        },
        "autuacao_disciplinar_historico": {
            "dbt_utils.relationships_where__id_auto_infracao__autuacao_disciplinar_historico": {
                "description": "Todos as autuações geraram lacre corretamente"
            },
        },
    }

    MONITORAMENTO_VEICULO_TEST = DBTTest(
        model="veiculo_fiscalizacao_lacre autuacao_disciplinar_historico",
        checks_list=MONITORAMENTO_VEICULO_CHECKS_LIST,
        truncate_date=True,
    )

    VEICULO_DIA_SELECTOR = DBTSelector(
        name="veiculo_dia",
        schedule_cron=create_daily_cron(hour=6),
        initial_datetime=datetime(2025, 6, 23, 0, 0, 0),
        incremental_delay_hours=24 * 7,
    )

    VEICULO_DIA_CHECKS_LIST = {
        "veiculo_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__data_id_veiculo__veiculo_dia": {
                "description": "Todos os registros são únicos"
            },
            "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__veiculo_dia": {
                "description": "Todas as datas possuem dados"
            },
        }
    }

    VEICULO_DIA_TEST = DBTTest(
        model="veiculo_dia",
        checks_list=VEICULO_DIA_CHECKS_LIST,
        truncate_date=True,
    )
