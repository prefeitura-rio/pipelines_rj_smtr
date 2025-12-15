# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Jaé
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import (
    create_daily_cron,
    create_hourly_cron,
    create_minute_cron,
)
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.pretreatment import raise_if_column_isna

JAE_SOURCE_NAME = "jae"
CLIENTE_TABLE_ID = "cliente"
GRATUIDADE_TABLE_ID = "gratuidade"
ESTUDANTE_TABLE_ID = "estudante"
LAUDO_PCD_TABLE_ID = "laudo_pcd"


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da Jaé
    """

    JAE_DATABASE_SETTINGS = {
        "principal_db": {
            "engine": "mysql",
            "host": "10.5.113.205",
        },
        "tarifa_db": {
            "engine": "postgresql",
            "host": "10.5.113.254",
        },
        "transacao_db": {
            "engine": "postgresql",
            "host": "10.5.114.104",
        },
        "tracking_db": {
            "engine": "postgresql",
            "host": "10.5.12.106",
        },
        "ressarcimento_db": {
            "engine": "postgresql",
            "host": "10.5.12.50",
        },
        "gratuidade_db": {
            "engine": "postgresql",
            "host": "10.5.14.19",
        },
        "fiscalizacao_db": {
            "engine": "postgresql",
            "host": "10.5.115.29",
        },
        "atm_gateway_db": {
            "engine": "postgresql",
            "host": "10.5.15.127",
        },
        "device_db": {
            "engine": "postgresql",
            "host": "10.5.114.114",
        },
        "erp_integracao_db": {
            "engine": "postgresql",
            "host": "10.5.12.105",
        },
        "financeiro_db": {
            "engine": "postgresql",
            "host": "10.5.12.109",
        },
        "midia_db": {
            "engine": "postgresql",
            "host": "10.5.12.52",
        },
        "processador_transacao_db": {
            "engine": "postgresql",
            "host": "10.5.14.59",
        },
        "atendimento_db": {
            "engine": "postgresql",
            "host": "10.5.14.170",
        },
        "gateway_pagamento_db": {
            "engine": "postgresql",
            "host": "10.5.113.130",
        },
        # "iam_db": {
        #     "engine": "mysql",
        #     "host": "10.5.13.201",
        # },
        "vendas_db": {
            "engine": "postgresql",
            "host": "10.5.114.15",
        },
    }

    JAE_SECRET_PATH = "smtr_jae_access_data"
    JAE_PRIVATE_BUCKET_NAMES = {"prod": "rj-smtr-jae-private", "dev": "rj-smtr-dev-private"}
    ALERT_WEBHOOK = "alertas_bilhetagem"
    RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID = "resultado_verificacao_captura_jae"

    TRANSACAO_TABLE_ID = "transacao"
    TRANSACAO_RIOCARD_TABLE_ID = "transacao_riocard"
    GPS_VALIDADOR_TABLE_ID = "gps_validador"
    INTEGRACAO_TABLE_ID = "integracao_transacao"
    TRANSACAO_ORDEM_TABLE_ID = "transacao_ordem"
    TRANSACAO_RETIFICADA_TABLE_ID = "transacao_retificada"
    LANCAMENTO_TABLE_ID = "lancamento"

    JAE_TABLE_CAPTURE_PARAMS = {
        TRANSACAO_TABLE_ID: {
            "query": """
                SELECT
                    *
                FROM
                    transacao
                WHERE
                    data_processamento >= timestamp '{start}' - INTERVAL '{delay} minutes'
                    AND data_processamento < timestamp '{end}' - INTERVAL '{delay} minutes'
            """,
            "database": "transacao_db",
            "capture_delay_minutes": {"0": 0, "2025-03-26 15:36:00": 5},
        },
        TRANSACAO_RETIFICADA_TABLE_ID: {
            "query": """
                SELECT
                    r.*,
                    t.data_transacao
                FROM
                    transacao_retificada r
                JOIN transacao t on r.id_transacao = t.id
                WHERE
                    data_retificacao >= timestamp '{start}' - INTERVAL '5 minutes'
                    AND data_retificacao < timestamp '{end}' - INTERVAL '5 minutes'
            """,
            "database": "transacao_db",
        },
        TRANSACAO_RIOCARD_TABLE_ID: {
            "query": """
                SELECT
                    *
                FROM
                    transacao_riocard
                WHERE
                    data_processamento >= timestamp '{start}' - INTERVAL '{delay} minutes'
                    AND data_processamento < timestamp '{end}' - INTERVAL '{delay} minutes'
            """,
            "database": "transacao_db",
            "capture_delay_minutes": {"0": 0, "2025-03-26 15:36:00": 5},
        },
        GPS_VALIDADOR_TABLE_ID: {
            "query": """
                SELECT
                    *
                FROM
                    tracking_detalhe
                WHERE
                    data_tracking >= timestamp '{start}' - INTERVAL '{delay} minutes'
                    AND data_tracking < timestamp '{end}' - INTERVAL '{delay} minutes'
            """,
            "database": "tracking_db",
            "capture_delay_minutes": {"0": 0, "2025-03-26 15:31:00": 10},
        },
        INTEGRACAO_TABLE_ID: {
            "database": "ressarcimento_db",
            "query": """
                SELECT
                    *
                FROM
                    integracao_transacao
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
                ORDER BY data_inclusao
            """,
        },
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
                    data_processamento >= '{start}'
                    AND data_processamento <= '{end}'
                    AND id_ordem_ressarcimento IS NOT NULL
                ORDER BY data_processamento
            """,
            "database": "transacao_db",
        },
        LANCAMENTO_TABLE_ID: {
            "query": """
                SELECT
                    l.*,
                    m.cd_tipo_movimento,
                    tm.ds_tipo_movimento,
                    tc.ds_tipo_conta,
                    tc.id_tipo_moeda,
                    tmo.descricao as tipo_moeda,
                    c.cd_cliente,
                    c.nr_logico_midia
                FROM
                    lancamento l
                LEFT JOIN
                    movimento m
                USING(id_movimento)
                LEFT JOIN
                    tipo_movimento tm
                USING(cd_tipo_movimento)
                LEFT JOIN
                    conta c
                USING(id_conta)
                LEFT JOIN
                    tipo_conta tc
                USING(cd_tipo_conta)
                LEFT JOIN
                    tipo_moeda tmo
                ON tc.id_tipo_moeda = tmo.id
                WHERE
                    l.dt_lancamento >= timestamp '{start}' - INTERVAL '{delay} minutes'
                    AND l.dt_lancamento < timestamp '{end}' - INTERVAL '{delay} minutes'
                ORDER BY l.dt_lancamento DESC

            """,
            "database": "financeiro_db",
            "capture_delay_minutes": {"0": 5},
        },
        "linha": {
            "query": """
                SELECT
                    *
                FROM
                    LINHA
            """,
            "database": "principal_db",
            "primary_keys": ["CD_LINHA"],
            "capture_flow": "auxiliar",
        },
        "produto": {
            "query": """
                SELECT
                    *
                FROM
                    PRODUTO
                WHERE
                    DT_INCLUSAO BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "principal_db",
            "primary_keys": ["CD_PRODUTO"],
            "capture_flow": "auxiliar",
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
            "primary_keys": ["CD_OPERADORA_TRANSPORTE"],
            "capture_flow": "auxiliar",
        },
        CLIENTE_TABLE_ID: {
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
            "primary_keys": ["CD_CLIENTE"],
            "pre_treatment_reader_args": {"dtype": {"NR_DOCUMENTO": "object"}},
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
            "capture_flow": "auxiliar",
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
            "primary_keys": ["CD_CLIENTE"],
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
            "capture_flow": "auxiliar",
        },
        GRATUIDADE_TABLE_ID: {
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
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "gratuidade_db",
            "primary_keys": ["id"],
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
            "capture_flow": "auxiliar",
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
            "primary_keys": ["CD_CONSORCIO"],
            "capture_flow": "auxiliar",
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
            "primary_keys": ["id"],
            "capture_flow": "auxiliar",
        },
        "linha_tarifa": {
            "query": """
                SELECT
                    *
                FROM
                    linha_tarifa
                WHERE
                    dt_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "tarifa_db",
            "primary_keys": [
                "cd_linha",
                "nr_sequencia",
            ],
            "capture_flow": "auxiliar",
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
            "primary_keys": [
                "CD_CONSORCIO",
                "CD_LINHA",
            ],
            "capture_flow": "auxiliar",
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
            "primary_keys": [
                "CD_CONSORCIO",
                "CD_OPERADORA_TRANSPORTE",
                "CD_LINHA",
            ],
            "capture_flow": "auxiliar",
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
            "primary_keys": [
                "NR_SEQ_ENDERECO",
            ],
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
            "capture_flow": "auxiliar",
        },
        ESTUDANTE_TABLE_ID: {
            "query": """
                SELECT
                    *
                FROM
                    estudante
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "gratuidade_db",
            "primary_keys": [],
            "capture_flow": "auxiliar",
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
            "first_timestamp": datetime(2025, 9, 16, 0, 0, 0),
        },
        "escola": {
            "query": """
                SELECT
                    *
                FROM
                    escola
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "gratuidade_db",
            "primary_keys": ["codigo_escola"],
            "capture_flow": "auxiliar",
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
            "first_timestamp": datetime(2025, 9, 16, 0, 0, 0),
        },
        LAUDO_PCD_TABLE_ID: {
            "query": """
                SELECT
                    *
                FROM
                    laudo_pcd
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "gratuidade_db",
            "primary_keys": ["id"],
            "capture_flow": "auxiliar",
            "save_bucket_names": JAE_PRIVATE_BUCKET_NAMES,
            "first_timestamp": datetime(2025, 9, 16, 0, 0, 0),
        },
        "ordem_ressarcimento": {
            "query": """
                SELECT
                    *
                FROM
                    ordem_ressarcimento
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "ressarcimento_db",
            "primary_keys": ["id"],
            "capture_flow": "ordem_pagamento",
            "pretreat_funcs": [raise_if_column_isna(column_name="id_ordem_pagamento")],
        },
        "ordem_pagamento": {
            "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "ressarcimento_db",
            "primary_keys": ["id"],
            "capture_flow": "ordem_pagamento",
        },
        "ordem_pagamento_consorcio_operadora": {
            "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento_consorcio_operadora
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "ressarcimento_db",
            "primary_keys": ["id"],
            "capture_flow": "ordem_pagamento",
        },
        "ordem_pagamento_consorcio": {
            "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento_consorcio
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "ressarcimento_db",
            "primary_keys": ["id"],
            "capture_flow": "ordem_pagamento",
        },
        "ordem_rateio": {
            "query": """
                SELECT
                    *
                FROM
                    ordem_rateio
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "ressarcimento_db",
            "primary_keys": ["id"],
            "capture_flow": "ordem_pagamento",
            "pretreat_funcs": [raise_if_column_isna(column_name="id_ordem_pagamento")],
        },
        "linha_sem_ressarcimento": {
            "query": """
                SELECT
                    *
                FROM
                    linha_sem_ressarcimento
                WHERE
                    dt_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            "database": "ressarcimento_db",
            "primary_keys": ["id_linha"],
            "capture_flow": "ordem_pagamento",
        },
    }

    TRANSACAO_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=TRANSACAO_TABLE_ID,
        first_timestamp=datetime(2025, 3, 21, 0, 0, 0),
        schedule_cron=create_minute_cron(),
        primary_keys=["id"],
    )

    TRANSACAO_RIOCARD_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=TRANSACAO_RIOCARD_TABLE_ID,
        first_timestamp=datetime(2025, 3, 21, 0, 0, 0),
        schedule_cron=create_minute_cron(),
        primary_keys=["id"],
    )

    TRANSACAO_RETIFICADA_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=TRANSACAO_RETIFICADA_TABLE_ID,
        first_timestamp=datetime(2025, 6, 3, 0, 0, 0),
        schedule_cron=create_minute_cron(minute=10),
        primary_keys=["id"],
        bucket_names=JAE_PRIVATE_BUCKET_NAMES,
    )

    GPS_VALIDADOR_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=GPS_VALIDADOR_TABLE_ID,
        first_timestamp=datetime(2025, 3, 26, 15, 30, 0),
        schedule_cron=create_minute_cron(),
        primary_keys=["id"],
    )

    LANCAMENTO_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=LANCAMENTO_TABLE_ID,
        first_timestamp=datetime(2025, 7, 21, 0, 0, 0),
        schedule_cron=create_minute_cron(),
        primary_keys=["id_lancamento"],
        bucket_names=JAE_PRIVATE_BUCKET_NAMES,
    )

    INTEGRACAO_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=INTEGRACAO_TABLE_ID,
        first_timestamp=datetime(2025, 3, 21, 0, 0, 0),
        schedule_cron=create_daily_cron(hour=5),
        primary_keys=["id"],
        max_recaptures=2,
        file_chunk_size=20000,
    )

    JAE_AUXILIAR_SOURCES = [
        SourceTable(
            source_name=JAE_SOURCE_NAME,
            table_id=k,
            first_timestamp=v.get("first_timestamp", datetime(2024, 1, 7, 0, 0, 0)),
            schedule_cron=create_hourly_cron(),
            primary_keys=v["primary_keys"],
            pretreatment_reader_args=v.get("pre_treatment_reader_args"),
            pretreat_funcs=v.get("pretreat_funcs"),
            bucket_names=v.get("save_bucket_names"),
            partition_date_only=v.get("partition_date_only", True),
            max_recaptures=v.get("max_recaptures", 60),
            raw_filetype=v.get("raw_filetype", "json"),
            file_chunk_size=v.get("file_chunk_size"),
        )
        for k, v in JAE_TABLE_CAPTURE_PARAMS.items()
        if v.get("capture_flow") == "auxiliar"
    ]

    ORDEM_PAGAMENTO_SOURCES = [
        SourceTable(
            source_name=JAE_SOURCE_NAME,
            table_id=k,
            first_timestamp=datetime(2024, 12, 30, 0, 0, 0),
            schedule_cron=create_daily_cron(hour=10),
            primary_keys=v["primary_keys"],
            pretreatment_reader_args=v.get("pretreatment_reader_args"),
            pretreat_funcs=v.get("pretreat_funcs"),
            bucket_names=v.get("bucket_names"),
            partition_date_only=v.get("partition_date_only", True),
            max_recaptures=v.get("max_recaptures", 60),
            raw_filetype=v.get("raw_filetype", "json"),
        )
        for k, v in JAE_TABLE_CAPTURE_PARAMS.items()
        if v.get("capture_flow") == "ordem_pagamento"
    ]

    TRANSACAO_ORDEM_SOURCE = SourceTable(
        source_name=JAE_SOURCE_NAME,
        table_id=TRANSACAO_ORDEM_TABLE_ID,
        first_timestamp=datetime(2024, 11, 21, 0, 0, 0),
        schedule_cron=create_daily_cron(hour=10),
        partition_date_only=True,
        max_recaptures=5,
        primary_keys=[
            "id",
            "id_ordem_ressarcimento",
            "data_processamento",
            "data_transacao",
        ],
        file_chunk_size=200000,
    )

    CLIENTE_SOURCE = [s for s in JAE_AUXILIAR_SOURCES if s.table_id == CLIENTE_TABLE_ID][0]
    GRATUIDADE_SOURCE = [s for s in JAE_AUXILIAR_SOURCES if s.table_id == GRATUIDADE_TABLE_ID][0]
    ESTUDANTE_SOURCE = [s for s in JAE_AUXILIAR_SOURCES if s.table_id == ESTUDANTE_TABLE_ID][0]
    LAUDO_PCD_SOURCE = [s for s in JAE_AUXILIAR_SOURCES if s.table_id == LAUDO_PCD_TABLE_ID][0]

    CHECK_CAPTURE_PARAMS = {
        TRANSACAO_TABLE_ID: {
            "source": TRANSACAO_SOURCE,
            "datalake_table": "rj-smtr.bilhetagem_staging.transacao",
            "timestamp_column": "data_processamento",
            "primary_keys": TRANSACAO_SOURCE.primary_keys,
            "final_timestamp_exclusive": True,
        },
        TRANSACAO_RIOCARD_TABLE_ID: {
            "source": TRANSACAO_RIOCARD_SOURCE,
            "datalake_table": "rj-smtr.bilhetagem_staging.transacao_riocard",
            "timestamp_column": "data_processamento",
            "primary_keys": TRANSACAO_RIOCARD_SOURCE.primary_keys,
            "final_timestamp_exclusive": True,
        },
        GPS_VALIDADOR_TABLE_ID: {
            "source": GPS_VALIDADOR_SOURCE,
            "datalake_table": "rj-smtr.monitoramento_staging.gps_validador",
            "timestamp_column": "data_tracking",
            "primary_keys": GPS_VALIDADOR_SOURCE.primary_keys,
            "final_timestamp_exclusive": True,
        },
        LANCAMENTO_TABLE_ID: {
            "source": LANCAMENTO_SOURCE,
            "datalake_table": "rj-smtr.bilhetagem_interno_staging.lancamento",
            "timestamp_column": "dt_lancamento",
            "primary_keys": [
                "ifnull(id_lancamento, concat(string(dt_lancamento), '_', id_movimento))",
                "id_conta",
            ],
            "final_timestamp_exclusive": True,
        },
        CLIENTE_TABLE_ID: {
            "source": CLIENTE_SOURCE,
            "datalake_table": "rj-smtr.cadastro_interno_staging.cliente",
            "timestamp_column": "dt_cadastro",
            "primary_keys": CLIENTE_SOURCE.primary_keys,
            "final_timestamp_exclusive": False,
        },
        GRATUIDADE_TABLE_ID: {
            "source": GRATUIDADE_SOURCE,
            "datalake_table": "rj-smtr.bilhetagem_staging.gratuidade",
            "timestamp_column": "data_inclusao",
            "primary_keys": GRATUIDADE_SOURCE.primary_keys,
            "final_timestamp_exclusive": False,
        },
        ESTUDANTE_TABLE_ID: {
            "source": ESTUDANTE_SOURCE,
            "datalake_table": "rj-smtr.bilhetagem_staging.estudante",
            "timestamp_column": "data_inclusao",
            "primary_keys": ["cd_cliente", "data_inclusao"],
            "final_timestamp_exclusive": False,
        },
        LAUDO_PCD_TABLE_ID: {
            "source": LAUDO_PCD_SOURCE,
            "datalake_table": "rj-smtr.bilhetagem_staging.laudo_pcd",
            "timestamp_column": "data_inclusao",
            "primary_keys": LAUDO_PCD_SOURCE.primary_keys,
            "final_timestamp_exclusive": False,
        },
    }

    BACKUP_BILLING_PAY_FOLDER = "backup_jae_billingpay"

    BACKUP_BILLING_LAST_VALUE_REDIS_KEY = "last_backup_value"

    BACKUP_JAE_BILLING_PAY = {
        "principal_db": {
            "exclude": [
                "LINHA",
                "OPERADORA_TRANSPORTE",
                "CLIENTE",
                "PESSOA_FISICA",
                "CONSORCIO",
                "LINHA_CONSORCIO",
                "LINHA_CONSORCIO_OPERADORA_TRANSPORTE",
                "ENDERECO",
                "check_cadastro_pcd_validado",
                "importa_pcd_pf",
                "gratuidade_import_pcd",
                "recarga_duplicada",
                "SEQUENCIA_SERVICO",
                "CLIENTE_FRAUDE_05092024",
                "stops_with_routes",
                "cliente_com_data_nascimento",
                "vt_verificar_cpf_setempedido_cartao",
                "Linhas_empresa_csv",
                "acerto_pedido_2",
                "routes",
                "fare_rules",
                "estudante_12032025",
                "temp_estudante_cpfduplicado_14032025",
                "estudante_11042025",
                "estudante_01042025",
                "temp_estudante_cpfduplicado_11042025",
                "estudante_30042025",
                "estudante_24062025",
                "estudante_20062025",
                "producao_20250617081705_02_VT",
                "temp_estudante_15082025",
                "estudante_11072025",
                "temp_cliente_02082025",
                "temp_requisicao_pedido_ticketeira",
                "temp_estudante_27082025",
                "temp_pedido_VT_12092025",
                "temp_estudante_02102025",
                "temp_VT_sem_retorno_16102025",
                "temp_cliente_fraudes_14102025",
                "temp_vt_sem_retorno_13102025",
                "temp_bloqueio_cliente_14102025",
                "temp_estudante_31102025",
                "temp_logistica_limbo_25102025",
                "temp_cartoes_transferidos_valorados_18112025",
                "temp_vt_limbo_24102025",
            ],
            "filter": {
                "ITEM_PEDIDO": ["DT_INCLUSAO"],
                "CLIENTE_CONTA_ACESSO": ["DT_INCLUSAO"],
                "CLIENTE_PERFIL": ["DT_CADASTRO"],
                "PEDIDO": [
                    "DT_CONCLUSAO_PEDIDO",
                    "DT_CANCELAMENTO",
                    "DT_PAGAMENTO",
                    "DT_INCLUSAO",
                ],
                "SERVICO_MOTORISTA": [
                    "DT_ABERTURA",
                    "DT_FECHAMENTO",
                ],
                "CONTROLE_PAGAMENTO_PEDIDO": [
                    "DT_PAGAMENTO",
                    "DT_BAIXA",
                    "DT_CREDITO",
                    "DT_INCLUSAO",
                ],
                "RESUMO_FECHAMENTO_SERVICO": [
                    "DT_ABERTURA",
                    "DT_FECHAMENTO",
                ],
                "CLIENTE_IMAGEM": [
                    "DT_INCLUSAO",
                    "DT_ALTERACAO",
                ],
                "IMPORTA_DET_LOTE_VT": ["DT_INCLUSAO"],
                "ITEM_PEDIDO_ENDERECO": ["DT_INCLUSAO"],
                "CLIENTE_FAVORECIDO": [
                    "DT_CANCELAMENTO",
                    "DT_INCLUSAO",
                ],
                "IMPORTA_DET_LOTE_VT_ERRO": ["DT_INCLUSAO"],
                "ERRO_IMPORTACAO_COLABORADOR_DETALHE": ["DT_CRIACAO"],
                "ERRO_IMPORTACAO_COLABORADOR": ["CD_ERRO"],
                "IMPORTA_LOTE_VT": ["DT_INCLUSAO"],
                "PESSOA_JURIDICA": ["CD_CLIENTE"],
                "ERRO_IMPORTACAO_PEDIDO_DETALHE": ["DT_CRIACAO"],
                "ERRO_IMPORTACAO_PEDIDO": ["CD_ERRO"],
                "MOTORISTA_OPERADORA": [
                    "DT_ASSOCIACAO",
                    "DT_FIM_ASSOCIACAO",
                ],
                "MOTORISTA": ["CD_MOTORISTA"],
                "IMPORTACAO_ARQUIVO": ["DT_INCLUSAO"],
                "GRUPO_LINHA": [
                    "DT_FIM_VALIDADE",
                    "DT_INCLUSAO",
                ],
                "CLIENTE_DEPENDENTE": [
                    "DT_INCLUSAO",
                    "DT_CANCELAMENTO",
                ],
                "pcd_mae": ["count(*)"],
            },
            "custom_select": {
                "CLIENTE_IMAGEM": """
                    select
                        *
                    from CLIENTE_IMAGEM
                    where ID_CLIENTE_IMAGEM IN (
                        select distinct
                            ID_CLIENTE_IMAGEM
                        from CLIENTE_IMAGEM
                        where {filter}
                    )
                """,
            },
            "page_size": {"CLIENTE_IMAGEM": 500},
        },
        "tarifa_db": {
            "exclude": ["linha_tarifa"],
            "filter": {
                "matriz_integracao": ["dt_inclusao"],
            },
        },
        "transacao_db": {
            "exclude": [
                "transacao",
                "transacao_riocard",
                "embossadora_producao_20240809",
                "transacao_faltante_23082023",
                # sem permissão #
                "temp_estudante_cpfduplicado_13032025",
                "temp_estudante_cpfduplicado_14032025",
                "temp_estudante_cpfduplicado_17032025",
            ],
            "filter": {
                "confirmacao_envio_pms": ["data_confirmacao"],
                "spatial_ref_sys": ["count(*)"],
                "us_rules": ["count(*)"],
                "us_lex": ["count(*)"],
                "us_gaz": ["count(*)"],
                "midia_jall": ["count(*)"],
            },
        },
        "tracking_db": {
            "exclude": [
                "tracking_detalhe",
            ],
            "filter": {
                "tracking_sumarizado": ["ultima_data_tracking"],
                "spatial_ref_sys": ["srid"],
                "mq_connections": ["count(*)"],
            },
        },
        "ressarcimento_db": {
            "exclude": [
                "integracao_transacao",
                "ordem_ressarcimento",
                "ordem_pagamento",
                "ordem_pagamento_consorcio_operadora",
                "ordem_pagamento_consorcio",
                "ordem_rateio",
                "linha_sem_ressarcimento",
                "percentual_rateio_integracao",
            ],
            "filter": {
                "item_ordem_transferencia_custodia": ["data_inclusao"],
                "batch_step_execution": [
                    "create_time",
                    "last_updated",
                ],
                "batch_step_execution_context": ["step_execution_id"],
                "batch_job_execution_params": ["job_execution_id"],
                "item_ordem_transferencia_custodia_old": ["data_inclusao"],
                "batch_job_instance": ["job_instance_id"],
                "batch_job_execution": [
                    "create_time",
                    "last_updated",
                ],
                "batch_job_execution_context": ["job_execution_id"],
            },
        },
        "gratuidade_db": {
            "exclude": [
                "gratuidade",
                "estudante_import_old",
                "estudante_import_old",
                "gratuidade_import_pcd_old",
                "estudante_seeduc_25032025",
                # sem permissão: #
                "pcd_excluir",
                "estudante_seeduc",
                "pcd_nao_excluir",
                "estudante_import_seeduc",
                "check_cadastro_pcd_validar",
                "gratuidade_import_pcd",
                "estudante_seeduc_nov2024",
                "check_cadastro_total1",
                "check_cadastro_pcd_validado",
                "estudante_federal",
                "estudante_sme_2025",
                "estudante_universitario",
                "estudante_sme_2025_2102",
                "temp_estudante_cpfduplicado_13032025",
                "temp_estudante_cpfduplicado_14032025",
                "temp_estudante_cpfduplicado_17032025",
                "estudante_sme_21012025",
                "estudante_sme_21022025",
                "temp_estudante_acerto_20032025",
                "estudante_universitario_24012025",
                "estudante_sme_17032025",
                "estudante_sme_31032025",
                "estudante_universitario_25032025",
                "estudante_universitario_25042025",
                "estudante_universitario_12032025",
                "estudante_seeduc_27062025",
                "estudante_seeduc_07082025",
                "estudante_universitario_10092025",
                "estudante_seeduc_22102025",
                "temp_estudante_seeduc_inativos_02122025",
                "estudante_universitario_24112025",
            ],
            "filter": {
                "lancamento_conta_gratuidade": ["data_inclusao"],
                "historico_status_gratuidade": ["data_inclusao"],
                "regra_gratuidade": ["data_fim_validade", "data_inclusao"],
                "conta_gratuidade": [
                    "data_cancelamento",
                    "data_ultima_atualizacao",
                    "data_inclusao",
                ],
                "estudante_prefeitura": ["id"],
                "estudante_anterior": ["data_inclusao"],
                "estudante": ["data_inclusao"],
                "laudo_pcd_cid": ["data_inclusao"],
                "laudo_pcd": ["data_inclusao"],
                "pcd": ["data_inclusao"],
                "laudo_pcd_tipo_doenca": ["data_inclusao"],
                "escola": ["data_inclusao"],
                "estudante_sme": ["count(*)"],
                "escola_importa": ["count(*)"],
                "cid_nova": ["count(*)"],
                "cid": ["count(*)"],
            },
        },
        "fiscalizacao_db": {
            "filter": {"fiscalizacao": ["dt_inclusao"]},
        },
        "atm_gateway_db": {
            "filter": {
                "requisicao": [
                    "dt_requisicao",
                    "dt_resposta",
                ]
            }
        },
        "device_db": {
            "filter": {
                "device_operadora_grupo": ["data_desassociacao", "data_inclusao"],
                "device_operadora": ["data_desassociacao", "data_inclusao"],
                "device": ["data_inclusao", "data_ultimo_comando"],
                "grupo_controle_device": ["data_inclusao"],
            }
        },
        "erp_integracao_db": {},
        "financeiro_db": {
            "exclude": [
                "sequencia_lancamento",
                "cliente_fraude_05092024",
                "cargas_garota_vip_18082023",
                "lancamento",
            ],
            "filter": {
                "conta": [
                    "dt_abertura",
                    "dt_fechamento",
                    "dt_lancamento",
                ],
                "lote_credito_conta": [
                    "dt_abertura",
                    "dt_fechamento",
                    "dt_inclusao",
                ],
                "evento_recebido": ["dt_inclusao"],
                "movimento": ["dt_movimento"],
                "evento_processado": ["dt_inclusao"],
                "evento_erro": ["dt_inclusao"],
                "midia_gravacao_fisica_141": ["dt_gravacao"],
                "midia_gravacao_fisica_148": ["id"],
                "midia_gravacao_fisica_145": ["id"],
                "midia_gravacao_fisica_136": ["dt_gravacao"],
                "midia_gravacao_fisica_142": ["dt_gravacao"],
                "midia_gravacao_fisica_140": ["dt_gravacao"],
                "midia_gravacao_fisica_137": ["dt_gravacao"],
                "midia_gravacao_fisica_138": ["dt_gravacao"],
                "midia_gravacao_fisica_135": ["dt_gravacao"],
                "midia_gravacao_fisica_133": ["dt_gravacao"],
                "midia_gravacao_fisica_139": ["dt_gravacao"],
                "criar_conta_financeira": ["count(*)"],
            },
            "custom_select": {
                "conta": """
                    select
                        *
                    from conta c
                    left join (
                        select
                            id_conta,
                            max(dt_lancamento) as dt_lancamento
                            from lancamento
                            group by id_conta
                    ) l using(id_conta)
                """,
                "lote_credito_conta": """
                    select
                        lcc.*,
                        lc.dt_abertura,
                        lc.dt_fechamento,
                        lc.dt_inclusao
                    from lote_credito_conta lcc
                    left join lote_credito lc using(id_lote_credito)
                """,
            },
        },
        "midia_db": {
            "exclude": [
                "midia_chip_12092024",
                "midia_chip_30092024",
                "cargas_garota_vip_18082023",
                # sem permissão #
                "tb_arquivos_validacao",
                "jal_sp_cbd_producao_tudo",
                "midia_chip",
                "midia_chip_12122024",
                "midia_gravacao_fisica_150",
                "midia_gravacao_fisica",
                "midia_gravacao_fisica_151",
                "jall_midia_erro",
                "jall_midia_nao_recebida",
                "erros_504_criacao_dock",
                "temp_estudante_cpfduplicado_13032025",
                "temp_estudante_cpfduplicado_14032025",
                "temp_estudante_cpfduplicado_17032025",
                "temp_midias_gratuidade_utilizacao_0107a1208",
                "temp_midia_limbo_nv",
                "temp_midia_limbo_09072025",
                "temp_uids_01",
                "temp_cartoes_duplicados_14082025",
                "temp_limbo_25102025",
            ],
            "filter": {
                "midia_evento": ["dt_inclusao"],
                "midia": [
                    "dt_cancelamento_logico",
                    "dt_cancelamento_fisico",
                    "dt_gravacao",
                    "dt_inclusao",
                ],
                "midia_cliente": [
                    "dt_associacao",
                    "dt_desassociacao",
                ],
                "midia_nova": [
                    "dt_cancelamento_logico",
                    "dt_cancelamento_fisico",
                    "dt_gravacao",
                    "dt_inclusao",
                ],
                "midia_backup": [
                    "dt_cancelamento_logico",
                    "dt_cancelamento_fisico",
                    "dt_gravacao",
                    "dt_inclusao",
                ],
                "midia_gravacao_fisica_141": ["id"],
                "midia_gravacao_fisica_148": ["id"],
                "midia_gravacao_fisica_145": ["id"],
                "midia_gravacao_fisica_142": ["dt_gravacao"],
                "midia_gravacao_fisica_140": ["dt_gravacao"],
                "retorno_geral": ["count(*)"],
                "midia_jall": ["count(*)"],
                "temp_retorno_midia": ["count(*)"],
            },
        },
        "processador_transacao_db": {
            "filter": {
                "transacao_erro": ["dt_inclusao"],
                "transacao_processada": ["dt_inclusao"],
                "transacao_recebida": ["dt_inclusao"],
            }
        },
        "atendimento_db": {},
        "gateway_pagamento_db": {
            "filter": {
                "payment_processing": ["created_at"],
                "card_processing": ["created_at"],
                "cnab_transaction": ["count(*)"],
            },
        },
        # "iam_db": {
        #     "exclude": [
        #         "gratuidade_import_pcd",
        #         "CLIENTE_FRAUDE_05092024",
        #     ],
        #     "filter": {
        #         "CONTROLE_CODIGO_VERIFICACAO": ["NR_SEQ"],
        #         "PERFIL_ACESSO": ["DT_INCLUSAO", "DT_CANCELAMENTO"],
        #         "SEGURANCA_CONTA_ACESSO": ["DT_INCLUSAO"],
        #         "CONTA_ACESSO": ["DT_EXPIRACAO", "DT_INCLUSAO"],
        #         "ATIVACAO_CONTA_ACESSO": ["CRIADO_EM", "DT_ATIVACAO"],
        #         "CONTA_ACESSO_BACK": ["DT_EXPIRACAO", "DT_INCLUSAO"],
        #         "check_cadastro_pcd_validado": ["data_nascimento"],
        #     },
        # },
        "vendas_db": {
            "exclude": ["nsu_temp_venda", "vendas_piu"],
            "filter": {
                "venda": [
                    "dt_cancelamento",
                    "dt_pagamento",
                    "dt_credito",
                    "dt_venda",
                ]
            },
        },
    }

    BACKUP_JAE_BILLING_PAY_HISTORIC = {
        "principal_db": {
            "CLIENTE_IMAGEM": {
                "start": datetime(2023, 6, 13, 15, 0, 0),
                "end": datetime(2025, 2, 26, 0, 0, 0),
            },
        },
        "processador_transacao_db": {
            "transacao_erro": {
                "start": datetime(2023, 7, 17, 15, 0, 0),
                "end": datetime(2025, 2, 26, 0, 0, 0),
            },
            "transacao_processada": {
                "start": datetime(2023, 7, 17, 15, 0, 0),
                "end": datetime(2025, 2, 26, 0, 0, 0),
            },
            "transacao_recebida": {
                "start": datetime(2023, 7, 17, 15, 0, 0),
                "end": datetime(2025, 2, 26, 0, 0, 0),
            },
        },
    }
