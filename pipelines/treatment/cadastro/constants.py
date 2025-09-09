# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de cadastro
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron, create_hourly_cron
from pipelines.treatment.templates.utils import DBTSelector, DBTTest


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados de cadastro
    """

    CADASTRO_SELECTOR = DBTSelector(
        name="cadastro",
        schedule_cron=create_hourly_cron(minute=10),
        initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
    )

    CADASTRO_VEICULO_SELECTOR = DBTSelector(
        name="cadastro_veiculo",
        schedule_cron=create_daily_cron(hour=6),
        initial_datetime=datetime(2025, 6, 23, 0, 0, 0),
    )

    SNAPSHOT_CADASTRO_VEICULO_SELECTOR = DBTSelector(
        name="snapshot_cadastro_veiculo",
    )

    CADASTRO_VEICULO_CHECKS_LIST = {
        "veiculo_licenciamento_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__veiculo_licenciamento_dia": {
                "description": "Todos os registros são únicos"
            },
            "test_check_data_arquivo_licenciamento__veiculo_licenciamento_dia": {
                "description": "Todos os dados usam a data de arquivo de licenciamento mais recente"
            },
        }
    }

    CADASTRO_VEICULO_TEST = DBTTest(
        model="veiculo_licenciamento_dia",
        checks_list=CADASTRO_VEICULO_CHECKS_LIST,
        truncate_date=True,
    )
