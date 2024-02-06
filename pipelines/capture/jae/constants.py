# -*- coding: utf-8 -*-
"""
Constant values for jae data capture
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants
from pipelines.utils.incremental_capture_strategy import DatetimeIncremental


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

    JAE_PRIVATE_BUCKET = "rj-smtr-jae-private"

    JAE_RAW_FILETYPE = "json"

    TRANSACAO_DEFAULT_PARAMS = {
        "table_id": "transacao",
        "raw_filetype": JAE_RAW_FILETYPE,
        "incremental_capture_strategy": DatetimeIncremental(
            max_incremental_window={"hours": 3}
        ).to_dict(),
        "data_extractor_params": {
            "database": "transacao_db",
            "query": """
                SELECT
                    *
                FROM
                    transacao
                WHERE
                    data_processamento <= '{{ end }}'
                {% if is_incremental() %}
                    AND data_processamento > '{{ start }}'
                {% endif %}
            """,
        },
        "primary_keys": ["id"],
    }

    AUXILIAR_GENERAL_CAPTURE_PARAMS = {
        "incremental_capture_strategy": DatetimeIncremental(
            max_incremental_window={"hours": 5}
        ).to_dict(),
        "raw_filetype": JAE_RAW_FILETYPE,
    }

    AUXILIAR_TABLE_CAPTURE_PARAMS = [
        {
            "table_id": "linha",
            "data_extractor_params": {
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
            "primary_keys": ["CD_LINHA"],
        },
        {
            "table_id": "operadora_transporte",
            "data_extractor_params": {
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
            "primary_keys": ["CD_OPERADORA_TRANSPORTE"],
        },
        {
            "table_id": "consorcio",
            "data_extractor_params": {
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
            "primary_keys": ["CD_CONSORCIO"],
        },
        {
            "table_id": "cliente",
            "data_extractor_params": {
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
            "primary_keys": ["CD_CLIENTE"],
            "save_bucket_name": JAE_PRIVATE_BUCKET,
        },
        {
            "project": smtr_constants.BILHETAGEM_DATASET_ID.value,
            "table_id": "percentual_rateio_integracao",
            "data_extractor_params": {
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
            "primary_keys": ["id"],
        },
        {
            "table_id": "conta_bancaria",
            "data_extractor_params": {
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
            "primary_keys": ["CD_CLIENTE"],
            "save_bucket_name": JAE_PRIVATE_BUCKET,
        },
        {
            "table_id": "contato_pessoa_juridica",
            "data_extractor_params": {
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
            "primary_keys": [
                "NR_SEQ_CONTATO",
                "CD_CLIENTE",
            ],
            "save_bucket_name": JAE_PRIVATE_BUCKET,
        },
    ]
