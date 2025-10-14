# -*- coding: utf-8 -*-
"""
Valores constantes para exportação das transações do BQ para o Postgres
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para exportação das transações do BQ para o Postgres
    """

    TRANSACAO_POSTGRES_TABLE_NAME = "transacao_bigquery"
