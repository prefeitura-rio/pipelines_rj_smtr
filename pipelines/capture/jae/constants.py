# -*- coding: utf-8 -*-
"""
Constant values for jae data capture
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants
from pipelines.utils.incremental_strategy import DatetimeIncremental


class constants(Enum):
    """
    Constant values for jae data capture
    """

    JAE_SOURCE_NAME = "jae"

    JAE_DATABASES = {
        "principal_db": {
            "engine": "mysql",
            "host": "10.5.114.121",
        },
        "tarifa_db": {
            "engine": "postgresql",
            "host": "10.5.113.254",
        },
        "transacao_db": {
            "engine": "postgresql",
            "host": "10.5.115.1",
        },
        "tracking_db": {
            "engine": "postgresql",
            "host": "10.5.15.25",
        },
        "ressarcimento_db": {
            "engine": "postgresql",
            "host": "10.5.15.127",
        },
    }

    BILHETAGEM_PRIVATE_BUCKET = "rj-smtr-jae-private"

    TRANSACAO_CAPTURE_PARAMS = {
        "source_name": JAE_SOURCE_NAME,
        "project": smtr_constants.BILHETAGEM_DATASET_ID.value,
        "table_id": "transacao",
        "partition_date_only": False,
        "incremental_strategy": DatetimeIncremental(max_incremental_window={"hours": 3}).to_dict(),
        "extract_params": {
            "database": "transacao_db",
            "query": """
                SELECT
                    *
                FROM
                    transacao
                {% if is_incremental() %}
                WHERE
                    data_processamento > '{{ start }}'
                    AND data_processamento <= '{{ end }}'
                {% endif %}
            """,
        },
        "primary_key": "id",
    }

    AUXILIAR_GENERAL_CAPTURE_PARAMS = {
        "source_name": JAE_SOURCE_NAME,
        "partition_date_only": True,
        "incremental_type": "datetime",
        "max_incremental_window": {"hours": 5},
    }

    AUXILIAR_TABLE_CAPTURE_PARAMS = [
        {
            "project": smtr_constants.BILHETAGEM_DATASET_ID.value,
            "table_id": "linha",
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        LINHA
                    {% if is_incremental() %}
                    WHERE
                        DT_INCLUSAO BETWEEN '{{ start }}'
                        AND '{{ end }}'
                    {% endif %}
                """,
            },
            "primary_key": "CD_LINHA",
        },
        {
            "project": smtr_constants.BILHETAGEM_DATASET_ID.value,
            "table_id": "operadora_transporte",
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        OPERADORA_TRANSPORTE
                    {% if is_incremental() %}
                    WHERE
                        DT_INCLUSAO BETWEEN '{{ start }}'
                        AND '{{ end }}'
                    {% endif %}
                """,
            },
            "primary_key": "CD_OPERADORA_TRANSPORTE",
        },
        {
            "project": smtr_constants.CADASTRO_DATASET_ID.value,
            "table_id": "consorcio",
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        CONSORCIO
                    {% if is_incremental() %}
                    WHERE
                        DT_INCLUSAO BETWEEN '{{ start }}'
                        AND '{{ end }}'
                    {% endif %}
                """,
            },
            "primary_key": "CD_CONSORCIO",
        },
        {
            "project": smtr_constants.CADASTRO_DATASET_ID.value,
            "table_id": "cliente",
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        c.*
                    FROM
                        CLIENTE c
                    {% if is_incremental() %}
                    WHERE
                        DT_CADASTRO BETWEEN '{{ start }}'
                        AND '{{ end }}'
                    {% endif %}
                """,
            },
            "primary_key": "CD_CLIENTE",
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
        {
            "project": smtr_constants.BILHETAGEM_DATASET_ID.value,
            "table_id": "percentual_rateio_integracao",
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                    SELECT
                        *
                    FROM
                        percentual_rateio_integracao
                    {% if is_incremental() %}
                    WHERE
                        dt_inclusao BETWEEN '{{ start }}'
                        AND '{{ end }}'
                    {% endif %}
                  """,
            },
            "primary_key": "id",
        },
        {
            "project": smtr_constants.CADASTRO_DATASET_ID.value,
            "table_id": "conta_bancaria",
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        c.*,
                        b.NM_BANCO
                    FROM
                        CONTA_BANCARIA c
                    JOIN
                        BANCO b
                    ON
                        b.NR_BANCO = c.NR_BANCO
                    JOIN
                        OPERADORA_TRANSPORTE o
                    ON
                        o.CD_CLIENTE = c.CD_CLIENTE
                    WHERE
                        {{ update }}
                """,
                "get_updates": [
                    "c.cd_cliente",
                    "c.cd_agencia",
                    "c.cd_tipo_conta",
                    "c.nr_banco",
                    "c.nr_conta",
                ],
            },
            "primary_key": "CD_CLIENTE",
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
        {
            "project": smtr_constants.CADASTRO_DATASET_ID.value,
            "table_id": "contato_pessoa_juridica",
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        CONTATO_PESSOA_JURIDICA
                    {% if is_incremental() %}
                    WHERE
                        DT_INCLUSAO BETWEEN '{{ start }}'
                        AND '{{ end }}'
                    {% endif %}
                """,
            },
            "primary_key": [
                "NR_SEQ_CONTATO",
                "CD_CLIENTE",
            ],
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
    ]
