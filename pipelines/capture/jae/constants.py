# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Jaé
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados da Jaé
    """

    JAE_SOURCE_NAME = "jae"

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

    TRANSACAO_ORDEM_SOURCE = SourceTable(
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
                "CONTATO_PESSOA_JURIDICA",
                "SERVICO_MOTORISTA",
                "LINHA_CONSORCIO",
                "LINHA_CONSORCIO_OPERADORA_TRANSPORTE",
                "ENDERECO",
                "CLIENTE_IMAGEM",
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
                # "CLIENTE_IMAGEM": [
                #     "DT_INCLUSAO",
                #     "DT_ALTERACAO",
                # ],
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
            },
        }
    }
