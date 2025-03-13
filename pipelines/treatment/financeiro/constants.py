# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados financeiros
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização dos dados financeiros
    """

    ORDEM_PAGAMENTO_CHECKS_LIST = {
        "ordem_pagamento_consorcio_operador_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_expectations.expect_column_max_to_be_between__data_ordem__ordem_pagamento_consorcio_operador_dia": {  # noqa
                "description": "A Ordem de pagamento está em dia"
            },
        }
    }
