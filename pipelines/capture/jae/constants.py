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
            "host": "10.5.115.153",
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
            "host": "10.5.14.228",
        },
        "fiscalizacao_db": {
            "engine": "postgresql",
            "host": "10.5.115.29",
        },
        "atm_gateway_db": {
            "engine": "postgresql",
            "host": "10.5.12.45",
        },
        "device_db": {
            "engine": "postgresql",
            "host": "10.5.112.161",
        },
        "erp_integracao_db": {
            "engine": "postgresql",
            "host": "10.5.12.105",
        },
        "financeiro_db": {
            "engine": "postgresql",
            "host": "10.5.12.203",
        },
        "midia_db": {
            "engine": "postgresql",
            "host": "10.5.12.62",
        },
        "processador_transacao_db": {
            "engine": "postgresql",
            "host": "10.5.12.185",
        },
        "atendimento_db": {
            "engine": "postgresql",
            "host": "10.5.14.170",
        },
        "gateway_pagamento_db": {
            "engine": "postgresql",
            "host": "10.5.113.179",
        },
        # "iam_db": {
        #     "engine": "mysql",
        #     "host": "10.5.13.201",
        # },
        "vendas_db": {
            "engine": "postgresql",
            "host": "10.5.113.30",
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
                "lancamento": ["dt_lancamento"],
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
            "filter": {
                "venda": [
                    "dt_cancelamento",
                    "dt_pagamento",
                    "dt_credito",
                    "dt_venda",
                ]
            }
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
