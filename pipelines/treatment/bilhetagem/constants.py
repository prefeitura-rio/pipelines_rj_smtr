# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de bilhetagem
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron, create_hourly_cron
from pipelines.treatment.templates.utils import DBTSelector, DBTTest


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de bilhetagem
    """

    TRANSACAO_SELECTOR = DBTSelector(
        name="transacao",
        schedule_cron=create_hourly_cron(minute=15),
        initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
        incremental_delay_hours=1,
    )

    TRANSACAO_POST_CHECKS_LIST = {
        "transacao": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
        "transacao_riocard": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "unique": {"description": "Todos os registros são únicos"},
        },
    }

    TRANSACAO_DAILY_TEST = DBTTest(
        model="transacao transacao_riocard",
        checks_list=TRANSACAO_POST_CHECKS_LIST,
        truncate_date=True,
        delay_days_start=1,
    )

    INTEGRACAO_SELECTOR = DBTSelector(
        name="integracao",
        schedule_cron=create_daily_cron(hour=8, minute=30),
        initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
    )

    INTEGRACAO_POST_CHECKS_LIST = {
        "integracao": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__integracao": {
                "description": "Todos os registros são únicos"
            },
        }
    }

    INTEGRACAO_DAILY_TEST = DBTTest(
        model="integracao",
        checks_list=INTEGRACAO_POST_CHECKS_LIST,
        truncate_date=True,
        delay_days_start=1,
    )

    PASSAGEIRO_HORA_SELECTOR = DBTSelector(
        name="passageiro_hora",
        schedule_cron=create_hourly_cron(minute=25),
        initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
    )

    PASSAGEIRO_HORA_POST_CHECKS_LIST = {
        "passageiro_hora": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
        "passageiro_tile_hora": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
        },
    }

    PASSAGEIRO_HORA_DAILY_TEST = DBTTest(
        model="passageiro_hora passageiro_tile_hora",
        checks_list=PASSAGEIRO_HORA_POST_CHECKS_LIST,
        truncate_date=True,
    )

    TRANSACAO_ORDEM_SELECTOR = DBTSelector(
        name="transacao_ordem",
        schedule_cron=create_daily_cron(hour=9),
        initial_datetime=datetime(2024, 11, 21, 0, 0, 0),
    )

    TRANSACAO_VALOR_ORDEM_SELECTOR = DBTSelector(
        name="transacao_valor_ordem",
        schedule_cron=create_daily_cron(hour=11, minute=30),
        initial_datetime=datetime(2025, 2, 4, 0, 0, 0),
    )

    TRANSACAO_VALOR_ORDEM_POST_CHECKS_LIST = {
        "transacao_valor_ordem": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__transacao_valor_ordem": {
                "description": "Todos os registros são únicos"
            },
            "transacao_valor_ordem_completa__transacao_valor_ordem": {
                "description": "Todas ordens estão presentes na tabela"
            },
        }
    }

    TRANSACAO_VALOR_ORDEM_DAILY_TEST = DBTTest(
        model="transacao_valor_ordem",
        checks_list=TRANSACAO_VALOR_ORDEM_POST_CHECKS_LIST,
        truncate_date=True,
    )
