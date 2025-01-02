# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Jaé
"""

from datetime import datetime
from enum import Enum

from pipelines.capture.templates.utils import DateRangeSourceTable, DefaultSourceTable
from pipelines.schedules import create_daily_cron, create_hourly_cron

JAE_SOURCE_NAME = "jae"


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da Jaé
    """

    JAE_SOURCE_NAME = JAE_SOURCE_NAME

    JAE_DATABASE_SETTINGS = {
        "principal_db": {
            "engine": "mysql",
            "host": "10.5.114.227",
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
            "host": "10.5.12.67",
        },
        "ressarcimento_db": {
            "engine": "postgresql",
            "host": "10.5.12.50",
        },
        "gratuidade_db": {
            "engine": "postgresql",
            "host": "10.5.12.107",
        },
        "fiscalizacao_db": {
            "engine": "postgresql",
            "host": "10.5.115.29",
        },
    }

    JAE_SECRET_PATH = "smtr_jae_access_data"

    JAE_PRIVATE_BUCKET_NAMES = {"prod": "rj-smtr-jae-private", "dev": "rj-smtr-dev-private"}

    JAE_AUXILIAR_CAPTURE_PARAMS = {
        "linha": {
            "query": """
                SELECT
                    *
                FROM
                    LINHA
                WHERE
                    DT_INCLUSAO BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "principal_db",
            "primary_key": ["CD_LINHA"],
        },
        "operadora_transporte": {
            "query": """
                SELECT
                    o.*,
                    m.DS_TIPO_MODAL
                FROM
                    OPERADORA_TRANSPORTE o
                LEFT JOIN
                    TIPO_MODAL m
                ON
                    o.CD_TIPO_MODAL = m.CD_TIPO_MODAL
                WHERE
                    DT_INCLUSAO BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "principal_db",
            "primary_key": ["CD_OPERADORA_TRANSPORTE"],
        },
        "cliente": {
            "query": """
                SELECT
                    c.*
                FROM
                    CLIENTE c
                WHERE
                    DT_CADASTRO BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "principal_db",
            "primary_key": ["CD_CLIENTE"],
            "pre_treatment_reader_args": {"dtype": {"NR_DOCUMENTO": "object"}},
        },
        "pessoa_fisica": {
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
                WHERE
                    c.DT_CADASTRO BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "principal_db",
            "primary_key": ["CD_CLIENTE"],
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        },
        "gratuidade": {
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
                WHERE
                    g.data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "gratuidade_db",
            "primary_key": ["id"],
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
        },
        "consorcio": {
            "query": """
                SELECT
                    *
                FROM
                    CONSORCIO
                WHERE
                    DT_INCLUSAO BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "principal_db",
            "primary_key": ["CD_CONSORCIO"],
        },
        "percentual_rateio_integracao": {
            "query": """
                SELECT
                    *
                FROM
                    percentual_rateio_integracao
                WHERE
                    dt_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "ressarcimento_db",
            "primary_key": ["id"],
        },
        "linha_consorcio": {
            "query": """
                SELECT
                    *
                FROM
                    LINHA_CONSORCIO
                WHERE
                    DT_INCLUSAO BETWEEN '{start}'
                    AND '{end}'
                    OR DT_FIM_VALIDADE BETWEEN DATE('{start}')
                    AND DATE('{end}')
            """,
            "database": "principal_db",
            "primary_key": [
                "CD_CONSORCIO",
                "CD_LINHA",
            ],
        },
        "linha_consorcio_operadora_transporte": {
            "query": """
                SELECT
                    *
                FROM
                    LINHA_CONSORCIO_OPERADORA_TRANSPORTE
                WHERE
                    DT_INCLUSAO BETWEEN '{start}'
                    AND '{end}'
                    OR DT_FIM_VALIDADE BETWEEN DATE('{start}')
                    AND DATE('{end}')
            """,
            "database": "principal_db",
            "primary_key": [
                "CD_CONSORCIO",
                "CD_OPERADORA_TRANSPORTE",
                "CD_LINHA",
            ],
        },
        "endereco": {
            "query": """
                SELECT
                    *
                FROM
                    ENDERECO
                WHERE
                    DT_INCLUSAO BETWEEN '{start}'
                    AND '{end}'
                    OR
                    DT_INATIVACAO BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "principal_db",
            "primary_key": [
                "NR_SEQ_ENDERECO",
            ],
        },
    }

    JAE_AUXILIAR_SOURCES = {
        k: DateRangeSourceTable(
            source_name=JAE_SOURCE_NAME,
            table_id=k,
            first_timestamp=datetime(2024, 12, 30, 0, 0, 0),
            schedule_cron=create_hourly_cron(),
            primary_keys=v["primary_keys"],
            pretreatment_reader_args=v.get("pretreatment_reader_args"),
            pretreat_funcs=v.get("pretreat_funcs"),
            bucket_names=v.get("bucket_names"),
            partition_date_only=v.get("partition_date_only", False),
            max_capture_hours=v.get("max_capture_hours", 5),
        )
        for k, v in JAE_AUXILIAR_CAPTURE_PARAMS
    }

    TRANSACAO_ORDEM_TABLE_ID = "transacao_ordem"

    JAE_TABLE_CAPTURE_PARAMS = {
        TRANSACAO_ORDEM_TABLE_ID: {
            "query": """
                SELECT
                    id,
                    id_ordem_ressarcimento,
                    data_processamento,
                    data_transacao
                FROM
                    transacao
                WHERE
                    DATE(data_processamento) >= DATE('{start}')
                    AND DATE(data_processamento) <= DATE('{end}')
                    AND id_ordem_ressarcimento IS NOT NULL
            """,
            "database": "transacao_db",
        }
    }

    TRANSACAO_ORDEM_SOURCE = DefaultSourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=TRANSACAO_ORDEM_TABLE_ID,
        first_timestamp=datetime(2024, 11, 21, 0, 0, 0),
        schedule_cron=create_daily_cron(hour=6),
        partition_date_only=True,
        max_recaptures=5,
        primary_keys=[
            "id",
            "id_ordem_ressarcimento",
            "data_processamento",
            "data_transacao",
        ],
    )

    ALERT_WEBHOOK = "alertas_bilhetagem"
