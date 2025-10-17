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
    REDIS_KEY = "cct_upload_transacao_bigquery"
    LAST_UPLOAD_TIMESTAMP_KEY_NAME = "last_upload_timestamp"
    TRANSACAO_CCT_FOLDER = "transacao_cct"
    EXPORT_GCS_PREFIX = f"upload/{TRANSACAO_CCT_FOLDER}"
    TRANSACAO_CCT_VIEW_NAME = "projeto_app_cct.transacao_cct"
