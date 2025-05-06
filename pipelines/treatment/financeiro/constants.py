# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados financeiros
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados financeiros
    """

    FINANCEIRO_BILHETAGEM_SELECTOR = DBTSelector(
        name="financeiro_bilhetagem",
        schedule_cron=create_daily_cron(hour=5, minute=15),
        initial_datetime=datetime(2025, 3, 26, 0, 0, 0),
    )

    ORDEM_PAGAMENTO_CHECKS_LIST = {
        "ordem_pagamento_consorcio_operador_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_expectations.expect_column_max_to_be_between__data_ordem__ordem_pagamento_consorcio_operador_dia": {  # noqa
                "description": "A Ordem de pagamento está em dia"
            },
        }
    }
