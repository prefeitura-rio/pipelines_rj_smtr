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
    TRANSACAO_POSTGRES_TMP_TABLE_NAME = f"tmp__{TRANSACAO_POSTGRES_TABLE_NAME}"
    REDIS_KEY = "cct_upload_transacao_bigquery"
    LAST_UPLOAD_TIMESTAMP_KEY_NAME = "last_upload_timestamp"
    TRANSACAO_CCT_FOLDER = "transacao_cct"
    EXPORT_GCS_PREFIX = f"upload/{TRANSACAO_CCT_FOLDER}"
    TRANSACAO_CCT_VIEW_NAME = "projeto_app_cct.transacao_cct"
    TMP_TABLE_INDEX_NAME = "idx_tmp_id_transacao"
    FINAL_TABLE_ID_TRANSACAO_INDEX_NAME = "idx_transacao_id_transacao"
    FINAL_TABLE_ID_ORDEM_INDEX_NAME = "idx_transacao_id_ordem"
    FINAL_TABLE_EXPORT_INDEX_NAME = "idx_transacao_datetime_export"
    TESTE_SINCRONIZACAO_TABLE_NAME = "teste_sincronizacao_transacao_cct"
