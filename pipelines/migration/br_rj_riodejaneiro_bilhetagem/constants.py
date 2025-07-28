# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_bilhetagem
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_bilhetagem
    """

    BILHETAGEM_PRIVATE_BUCKET = "rj-smtr-jae-private"

    BILHETAGEM_TRANSACAO_CAPTURE_PARAMS = {
        "table_id": "transacao",
        "partition_date_only": False,
        "extract_params": {
            "database": "transacao_db",
            "query": """
                SELECT
                    *
                FROM
                    transacao
                WHERE
                    data_processamento BETWEEN '{start}'
                    AND '{end}'
            """,
        },
        "primary_key": ["id"],
        "interval_minutes": 1,
    }

    BILHETAGEM_TRANSACAO_RIOCARD_CAPTURE_PARAMS = {
        "table_id": "transacao_riocard",
        "partition_date_only": False,
        "extract_params": {
            "database": "transacao_db",
            "query": """
                SELECT
                    *
                FROM
                    transacao_riocard
                WHERE
                    data_processamento >= '{start}'
                    AND data_processamento < '{end}'
            """,
        },
        "primary_key": ["id"],
        "interval_minutes": 1,
    }

    BILHETAGEM_FISCALIZACAO_CAPTURE_PARAMS = {
        "table_id": "fiscalizacao",
        "partition_date_only": False,
        "extract_params": {
            "database": "fiscalizacao_db",
            "query": """
                SELECT
                    *
                FROM
                    fiscalizacao
                WHERE
                    dt_inclusao >= '{start}'
                    AND dt_inclusao < '{end}'
            """,
        },
        "primary_key": ["id"],
        "interval_minutes": 5,
        "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
    }

    BILHETAGEM_INTEGRACAO_CAPTURE_PARAMS = {
        "table_id": "integracao_transacao",
        "partition_date_only": False,
        "extract_params": {
            "database": "ressarcimento_db",
            "query": """
                SELECT
                    *
                FROM
                    integracao_transacao
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
        },
        "primary_key": ["id"],
        "interval_minutes": 1440,
    }

    BILHETAGEM_ORDEM_PAGAMENTO_CAPTURE_PARAMS = [
        {
            "table_id": "ordem_ressarcimento",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    ordem_ressarcimento
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id"],
            "interval_minutes": 1440,
        },
        {
            "table_id": "ordem_pagamento",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id"],
            "interval_minutes": 1440,
        },
        {
            "table_id": "ordem_pagamento_consorcio_operadora",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento_consorcio_operadora
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id"],
            "interval_minutes": 1440,
        },
        {
            "table_id": "ordem_pagamento_consorcio",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    ordem_pagamento_consorcio
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id"],
            "interval_minutes": 1440,
        },
        {
            "table_id": "ordem_rateio",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    ordem_rateio
                WHERE
                    data_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id"],
            "interval_minutes": 1440,
        },
        {
            "table_id": "linha_sem_ressarcimento",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                SELECT
                    *
                FROM
                    linha_sem_ressarcimento
                WHERE
                    dt_inclusao BETWEEN '{start}'
                    AND '{end}'
            """,
            },
            "primary_key": ["id_linha"],
            "interval_minutes": 1440,
        },
    ]

    BILHETAGEM_SECRET_PATH = "smtr_jae_access_data"

    BILHETAGEM_TRATAMENTO_INTERVAL = 60

    BILHETAGEM_CAPTURE_PARAMS = [
        {
            "table_id": "linha",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        LINHA
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_LINHA"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "operadora_transporte",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
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
            },
            "primary_key": ["CD_OPERADORA_TRANSPORTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "cliente",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        c.*
                    FROM
                        CLIENTE c
                    WHERE
                        DT_CADASTRO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_CLIENTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
            "pre_treatment_reader_args": {"dtype": {"NR_DOCUMENTO": "object"}},
        },
        {
            "table_id": "pessoa_fisica",
            "partition_date_only": True,
            "extract_params": {
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
                    WHERE
                        c.DT_CADASTRO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_CLIENTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
        {
            "table_id": "gratuidade",
            "partition_date_only": True,
            "extract_params": {
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
                    WHERE
                        g.data_inclusao BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["id"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
        {
            "table_id": "consorcio",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        CONSORCIO
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_CONSORCIO"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "percentual_rateio_integracao",
            "partition_date_only": True,
            "extract_params": {
                "database": "ressarcimento_db",
                "query": """
                      SELECT
                          *
                      FROM
                          percentual_rateio_integracao
                      WHERE
                          dt_inclusao BETWEEN '{start}'
                          AND '{end}'
                  """,
            },
            "primary_key": ["id"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "conta_bancaria",
            "partition_date_only": True,
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
                        {update}
                """,
                "get_updates": [
                    "c.cd_cliente",
                    "c.cd_agencia",
                    "c.cd_tipo_conta",
                    "c.nr_banco",
                    "c.nr_conta",
                ],
            },
            "primary_key": ["CD_CLIENTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
        {
            "table_id": "contato_pessoa_juridica",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        CONTATO_PESSOA_JURIDICA
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": [
                "NR_SEQ_CONTATO",
                "CD_CLIENTE",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
        # {
        #     "table_id": "servico_motorista",
        #     "partition_date_only": True,
        #     "extract_params": {
        #         "database": "principal_db",
        #         "query": """
        #             SELECT
        #                 *
        #             FROM
        #                 SERVICO_MOTORISTA
        #             WHERE
        #                 DT_ABERTURA BETWEEN '{start}'
        #                 AND '{end}'
        #                 OR DT_FECHAMENTO BETWEEN '{start}'
        #                 AND '{end}'
        #         """,
        #     },
        #     "primary_key": [
        #         "NR_LOGICO_MIDIA",
        #         "ID_SERVICO",
        #     ],  # id column to nest data on
        #     "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        # },
        {
            "table_id": "linha_tarifa",
            "partition_date_only": True,
            "extract_params": {
                "database": "tarifa_db",
                "query": """
                    SELECT
                        *
                    FROM
                        linha_tarifa
                    WHERE
                        dt_inclusao BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": [
                "cd_linha",
                "nr_sequencia",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "linha_consorcio",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
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
            },
            "primary_key": [
                "CD_CONSORCIO",
                "CD_LINHA",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "linha_consorcio_operadora_transporte",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
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
            },
            "primary_key": [
                "CD_CONSORCIO",
                "CD_OPERADORA_TRANSPORTE",
                "CD_LINHA",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "endereco",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
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
            },
            "primary_key": [
                "NR_SEQ_ENDERECO",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
            "save_bucket_name": BILHETAGEM_PRIVATE_BUCKET,
        },
    ]

    BILHETAGEM_EXCLUDE = "+operadoras +consorcios +servicos"

    BILHETAGEM_JAE_DASHBOARD_DATASET_ID = "dashboard_bilhetagem_jae"

    BILHETAGEM_MATERIALIZACAO_INTEGRACAO_PARAMS = {
        "dataset_id": BILHETAGEM_JAE_DASHBOARD_DATASET_ID,
        "table_id": "view_integracao",
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data",
                "delay_hours": 0,
            },
            "version": {},
        },
        "exclude": f"{BILHETAGEM_EXCLUDE} stops_gtfs routes_gtfs feed_info_gtfs",
    }

    BILHETAGEM_MATERIALIZACAO_TRANSACAO_PARAMS = {
        "dataset_id": smtr_constants.BILHETAGEM_DATASET_ID.value,
        "table_id": "transacao",
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data",
                "delay_hours": 1,
            },
            "version": {},
        },
        "exclude": "integracao matriz_integracao gtfs \
ordem_pagamento_dia ordem_pagamento_consorcio_dia ordem_pagamento_consorcio_operador_dia \
staging_ordem_pagamento_consorcio staging_ordem_pagamento \
ordem_pagamento_servico_operador_dia staging_ordem_pagamento_consorcio_operadora \
aux_retorno_ordem_pagamento staging_arquivo_retorno aux_transacao_id_ordem_pagamento \
staging_transacao_ordem",
    }

    BILHETAGEM_MATERIALIZACAO_PASSAGEIROS_HORA_PARAMS = {
        "dataset_id": BILHETAGEM_JAE_DASHBOARD_DATASET_ID,
        "table_id": "view_passageiros_hora",
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data",
                "delay_hours": 0,
                "truncate_minutes": False,
            },
            "version": {},
        },
        "exclude": "+transacao",
        "source_dataset_ids": [],
        "source_table_ids": [],
        "capture_intervals_minutes": [],
    }

    BILHETAGEM_MATERIALIZACAO_DASHBOARD_CONTROLE_VINCULO_PARAMS = {
        "dataset_id": "dashboard_controle_vinculo_jae_riocard",
        "table_id": "veiculo_indicadores_dia",
        "upstream": True,
        "dbt_vars": {
            "run_date": {},
            "version": {},
        },
        "exclude": "+gps_sppo +sppo_licenciamento +gps_validador +transacao_riocard",
    }

    BILHETAGEM_MATERIALIZACAO_ORDEM_PAGAMENTO_PARAMS = {
        "dataset_id": smtr_constants.BILHETAGEM_DATASET_ID.value,
        "table_id": "ordem_pagamento_dia",
        "upstream": True,
        "exclude": BILHETAGEM_EXCLUDE,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data_ordem",
                "delay_hours": 0,
            },
            "version": {},
        },
    }

    BILHETAGEM_MATERIALIZACAO_GPS_VALIDADOR_GENERAL_PARAMS = {
        "dataset_id": smtr_constants.BILHETAGEM_DATASET_ID.value,
        "upstream": True,
        "downstream": True,
        "exclude": f"{BILHETAGEM_EXCLUDE} veiculo_validacao veiculo_indicadores_dia \
viagem_transacao+",
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "datetime_captura",
                "delay_hours": 0,
            },
            "version": {},
        },
    }

    BILHETAGEM_MATERIALIZACAO_GPS_VALIDADOR_TABLE_ID = "gps_validador"
    BILHETAGEM_MATERIALIZACAO_GPS_VALIDADOR_VAN_TABLE_ID = "gps_validador_van"

    BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS = {
        "dataset_id": smtr_constants.BILHETAGEM_DATASET_ID.value,
        "secret_path": BILHETAGEM_SECRET_PATH,
        "source_type": smtr_constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["source_type"],
    }

    BILHETAGEM_MATERIALIZACAO_VALIDACAO_JAE_PARAMS = {
        "dataset_id": "validacao_dados_jae",
        "upstream": True,
        "exclude": "gtfs +transacao +integracao",
        "dbt_vars": {
            "run_date": {},
            "version": {},
        },
    }

    ORDEM_PAGAMENTO_CONSORCIO_OPERADOR_DIA_CHECK_ID = "ordem-pagamento-consorcio-operador-dia"
