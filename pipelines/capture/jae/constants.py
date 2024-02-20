# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da JAE
"""

from enum import Enum

from pipelines.utils.incremental_capture_strategy import (
    DatetimeIncremental,
    IDIncremental,
)


class constants(Enum):
    """
    Valores constantes para captura de dados da JAE
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
        "gratuidade_db": {
            "engine": "postgresql",
            "host": "10.5.12.107",
        },
    }

    JAE_PRIVATE_BUCKET = {"dev": "br-rj-smtr-jae-private-dev", "prod": "br-rj-smtr-jae-private-dev"}

    JAE_RAW_FILETYPE = "json"

    TRANSACAO_DEFAULT_PARAMS = {
        "table_id": "transacao",
        "raw_filetype": JAE_RAW_FILETYPE,
        "incremental_capture_strategy": DatetimeIncremental(
            max_incremental_window={"hours": 3}, first_value="2024-02-15 00:00:00"
        ).to_dict(),
        "data_extractor_params": {
            "database": "transacao_db",
            "query": """
                SELECT
                    *
                FROM
                    transacao
                WHERE
                    data_processamento > '{{ start }}'
                    AND data_processamento <= '{{ end }}'
            """,
        },
        "primary_keys": ["id"],
    }

    GPS_VALIDADOR_CAPTURE_PARAMS = {
        "table_id": "gps_validador",
        "raw_filetype": JAE_RAW_FILETYPE,
        "incremental_capture_strategy": IDIncremental(
            max_incremental_window=100_000,
            id_column_name="id",
            first_value=406_064_585,
        ),
        "data_extractor_params": {
            "database": "tracking_db",
            "query": """
                SELECT
                    *
                FROM
                    tracking_detalhe
                WHERE
                    id > {{ start }} AND id <= {{ end }}
            """,
            "page_size": 1000,
            "max_pages": 100,
        },
        "primary_key": ["id"],
        "interval_minutes": 5,
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
            "save_bucket_names": JAE_PRIVATE_BUCKET,
        },
        {
            "table_id": "pessoa_fisica",
            "data_extractor_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        p.*,
                        c.DT_CADASTRO
                    FROM
                        PESSOA_FISICA p
                    JOIN
                        CLIENTE c
                    ON
                        p.CD_CLIENTE = c.CD_CLIENTE
                    {% if is_incremental() %}
                        WHERE
                            c.DT_CADASTRO BETWEEN '{{ start }}'
                            AND '{{ end }}'
                    {% endif %}
                """,
            },
            "primary_keys": ["CD_CLIENTE"],
            "save_bucket_names": JAE_PRIVATE_BUCKET,
        },
        {
            "table_id": "gratuidade",
            "data_extractor_params": {
                "database": "gratuidade_db",
                "query": """
                    SELECT
                        g.*,
                        t.descricao AS tipo_gratuidade
                    FROM
                        gratuidade g
                    LEFT JOIN
                        tipo_gratuidade t
                    ON
                        g.id_tipo_gratuidade = t.id
                    {% if is_incremental() %}
                        WHERE
                            g.data_inclusao BETWEEN '{{ start }}'
                            AND '{{ end }}'
                    {% endif %}
                """,
            },
            "primary_keys": ["id"],
            "save_bucket_names": JAE_PRIVATE_BUCKET,
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
            "save_bucket_names": JAE_PRIVATE_BUCKET,
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
            "save_bucket_names": JAE_PRIVATE_BUCKET,
        },
    ]
