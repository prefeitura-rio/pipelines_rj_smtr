# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr projeto_subsidio_sppo
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr projeto_subsidio_sppo
    """

    SUBSIDIO_SPPO_FINANCEIRO_DATASET_ID = "financeiro"

    SUBSIDIO_SPPO_DATASET_ID = "projeto_subsidio_sppo"
    SUBSIDIO_SPPO_SECRET_PATH = "projeto_subsidio_sppo"
    SUBSIDIO_SPPO_TABLE_ID = "viagem_completa"
    SUBSIDIO_SPPO_CODE_OWNERS = ["dados_smtr"]

    SUBSIDIO_SPPO_V2_DATASET_ID = "subsidio"
    # Feature Apuração por faixa horária
    DATA_SUBSIDIO_V9_INICIO = "2024-08-16"

    # SUBSÍDIO DASHBOARD
    # flake8: noqa: E501
    SUBSIDIO_SPPO_DASHBOARD_DATASET_ID = "dashboard_subsidio_sppo"
    SUBSIDIO_SPPO_DASHBOARD_V2_DATASET_ID = "dashboard_subsidio_sppo_v2"
    SUBSIDIO_SPPO_DASHBOARD_STAGING_DATASET_ID = "dashboard_subsidio_sppo_staging"
    SUBSIDIO_SPPO_DASHBOARD_TABLE_ID = "sumario_servico_dia"
    SUBSIDIO_SPPO_DASHBOARD_SUMARIO_TABLE_ID = "sumario_servico_dia_tipo"
    SUBSIDIO_SPPO_DASHBOARD_SUMARIO_TABLE_ID_V2 = "sumario_servico_dia_pagamento"
    SUBSIDIO_SPPO_DATA_CHECKS_PARAMS = {
        "check_trips_processing": {
            "query": """SELECT
                        s.data,
                        s.tipo_dia,
                        s.subtipo_dia,
                        s.tipo_os,
                        s.feed_version,
                        s.feed_start_date AS feed_start_date_invalido,
                        i.feed_start_date AS feed_start_date_valido,
                        FROM (
                            SELECT
                                *
                            FROM
                                rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
                            WHERE
                                DATA >= "2024-04-01" -- DATA_SUBSIDIO_V6_INICIO (Feature trajetos alternativos)
                                AND DATA BETWEEN DATE("{start_timestamp}")
                                    AND DATE("{end_timestamp}")
                                ) AS s
                        LEFT JOIN
                            rj-smtr.gtfs.feed_info AS i
                        ON
                        (DATA BETWEEN i.feed_start_date
                            AND i.feed_end_date
                            OR (DATA >= i.feed_start_date
                            AND i.feed_end_date IS NULL))
                        WHERE
                            i.feed_start_date != s.feed_start_date
                        """,
            "order_columns": ["data"],
        },
        "check_gps_capture": {
            "query": """WITH
            t AS (
            SELECT
                DATETIME(timestamp_array) AS timestamp_array
            FROM
                UNNEST( GENERATE_TIMESTAMP_ARRAY( TIMESTAMP("{start_timestamp}"), TIMESTAMP("{end_timestamp}"), INTERVAL {interval} minute) ) AS timestamp_array
            WHERE
                timestamp_array < TIMESTAMP("{end_timestamp}") ),
            logs_table AS (
            SELECT
                SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
                SAFE_CAST(sucesso AS BOOLEAN) sucesso,
                SAFE_CAST(erro AS STRING) erro,
                SAFE_CAST(DATA AS DATE) DATA
            FROM
                rj-smtr-staging.{dataset_id}_staging.{table_id}_logs AS t ),
            logs AS (
            SELECT
                *,
                TIMESTAMP_TRUNC(timestamp_captura, minute) AS timestamp_array
            FROM
                logs_table
            WHERE
                DATA BETWEEN DATE(TIMESTAMP("{start_timestamp}"))
                AND DATE(TIMESTAMP("{end_timestamp}"))
                AND timestamp_captura BETWEEN "{start_timestamp}"
                AND "{end_timestamp}" )
            SELECT
                COALESCE(logs.timestamp_captura, t.timestamp_array) AS timestamp_captura,
                logs.erro
            FROM
                t
            LEFT JOIN
                logs
            ON
                logs.timestamp_array = t.timestamp_array
            WHERE
                logs.sucesso IS NOT TRUE""",
            "order_columns": ["timestamp_captura"],
        },
        "check_gps_treatment": {
            "query": """
            WITH
            data_hora AS (
                SELECT
                    EXTRACT(date
                    FROM
                    timestamp_array) AS DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_array) AS hora,
                FROM
                    UNNEST(GENERATE_TIMESTAMP_ARRAY("{start_timestamp}", "{end_timestamp}", INTERVAL 1 hour)) AS timestamp_array ),
            gps_raw AS (
                SELECT
                    EXTRACT(date
                    FROM
                    timestamp_gps) AS DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_gps) AS hora,
                    COUNT(*) AS q_gps_raw
                FROM
                    `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_registros`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    1,
                    2 ),
            gps_filtrada AS (
                SELECT
                    EXTRACT(date
                            FROM
                            timestamp_gps) AS DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_gps) AS hora,
                    COUNT(*) AS q_gps_filtrada
                FROM
                    `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_aux_registros_filtrada`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    1,
                    2 ),
            gps_sppo AS (
                SELECT
                    DATA,
                    EXTRACT(hour
                    FROM
                    timestamp_gps) AS hora,
                    COUNT(*) AS q_gps_treated
                FROM
                    `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    1,
                    2),
            gps_join AS (
                SELECT
                    *,
                    SAFE_DIVIDE(q_gps_filtrada, q_gps_raw) as indice_tratamento_raw,
                    SAFE_DIVIDE(q_gps_treated, q_gps_filtrada) as indice_tratamento_filtrada,
                    CASE
                        WHEN    q_gps_raw = 0 OR q_gps_filtrada = 0 OR q_gps_treated = 0                -- Hipótese de perda de dados no tratamento
                                OR q_gps_raw IS NULL OR q_gps_filtrada IS NULL OR q_gps_treated IS NULL -- Hipótese de perda de dados no tratamento
                                OR (q_gps_raw <= q_gps_filtrada) OR (q_gps_filtrada < q_gps_treated)   -- Hipótese de duplicação de dados
                                OR (COALESCE(SAFE_DIVIDE(q_gps_filtrada, q_gps_raw), 0) < 0.96)         -- Hipótese de perda de dados no tratamento (superior a 3%)
                                OR (COALESCE(SAFE_DIVIDE(q_gps_treated, q_gps_filtrada), 0) < 0.96)     -- Hipótese de perda de dados no tratamento (superior a 3%)
                                THEN FALSE
                    ELSE
                    TRUE
                END
                    AS status
                FROM
                    data_hora
                LEFT JOIN
                    gps_raw
                USING
                    (DATA,
                    hora)
                LEFT JOIN
                    gps_filtrada
                USING
                    (DATA,
                    hora)
                LEFT JOIN
                    gps_sppo
                USING
                    (DATA,
                    hora))
            SELECT
                *
            FROM
                gps_join
            WHERE
                status IS FALSE
            """,
            "order_columns": ["DATA", "hora"],
        },
        "check_sppo_veiculo_dia": {
            "query": """
            WITH
                count_dist_status AS (
                SELECT
                    DATA,
                    COUNT(DISTINCT status) AS q_dist_status,
                    NULL AS q_duplicated_status,
                    NULL AS q_null_status
                FROM
                    rj-smtr.veiculo.sppo_veiculo_dia
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    1
                HAVING
                    COUNT(DISTINCT status) = 1 ),
                count_duplicated_status AS (
                SELECT
                    DATA,
                    id_veiculo,
                    COUNT(*) AS q_status,
                FROM
                    rj-smtr.veiculo.sppo_veiculo_dia
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    1,
                    2
                HAVING
                    COUNT(*) > 1 ),
                count_duplicated_status_agg AS (
                SELECT
                    DATA,
                    NULL AS q_dist_status,
                    SUM(q_status) AS q_duplicated_status,
                    NULL AS q_null_status
                FROM
                    count_duplicated_status
                GROUP BY
                    1),
                count_null_status AS (
                SELECT
                    DATA,
                    NULL AS q_dist_status,
                    NULL AS q_duplicated_status,
                    COUNT(*) AS q_null_status
                FROM
                    rj-smtr.veiculo.sppo_veiculo_dia
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                    AND status IS NULL
                GROUP BY
                    1 )
            SELECT
                *
            FROM
                count_dist_status

            UNION ALL

            SELECT
                *
            FROM
                count_duplicated_status_agg

            UNION ALL

            SELECT
                *
            FROM
                count_null_status
            """,
            "order_columns": ["DATA"],
        },
        "accepted_values_valor_penalidade": {
            "query": """
            WITH
                all_values AS (
                SELECT
                    DISTINCT valor_penalidade AS value_field,
                    COUNT(*) AS n_records
                FROM
                    `rj-smtr`.`{dataset_id2}`.`{table_id}`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    valor_penalidade )
                SELECT
                    *
                FROM
                    all_values
                WHERE
                    value_field NOT IN (
                        SELECT
                            valor
                        FROM
                            `rj-smtr`.`dashboard_subsidio_sppo`.`valor_tipo_penalidade` )
            """,
            "order_columns": ["n_records"],
        },
        "teto_pagamento_valor_subsidio_pago": {
            "query": """
                WITH
                    {table_id} AS (
                        SELECT
                            *
                        FROM
                            `rj-smtr`.`{dataset_id2}`.`{table_id}`
                        WHERE
                            DATA BETWEEN DATE("{start_timestamp}")
                            AND DATE("{end_timestamp}")),
                    subsidio_valor_km_tipo_viagem AS (
                        SELECT
                            data_inicio,
                            data_fim,
                            MAX(subsidio_km) AS subsidio_km_teto
                        FROM
                            `rj-smtr`.`dashboard_subsidio_sppo_staging`.`subsidio_valor_km_tipo_viagem`
                        WHERE
                            subsidio_km > 0
                        GROUP BY
                            1,
                            2)
                    SELECT
                        *
                    FROM
                        {table_id} AS s
                    LEFT JOIN
                        subsidio_valor_km_tipo_viagem AS p
                    ON
                        s.data BETWEEN p.data_inicio
                        AND p.data_fim
                    WHERE
                        NOT({expression})
            """,
            "order_columns": ["data"],
        },
        "expression_is_true": {
            "query": """
                SELECT
                    *
                FROM
                    `rj-smtr`.`{dataset_id2}`.`{table_id}`
                WHERE
                    (DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}"))
                    AND NOT({expression})
            """,
            "order_columns": ["data"],
        },
        "expression_is_true_viagens_remuneradas": {
            "query": """
                SELECT
                    *
                FROM
                    `rj-smtr`.`{dataset_id}`.`{table_id}`
                WHERE
                    (DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}"))
                    AND NOT({expression})
            """,
            "order_columns": ["data"],
        },
        "unique_combination": {
            "query": """
            SELECT
                {expression}
            FROM
                `rj-smtr`.`{dataset_id2}`.`{table_id}`
            WHERE
                DATA BETWEEN DATE("{start_timestamp}")
                AND DATE("{end_timestamp}")
            GROUP BY
                {expression}
            HAVING
                COUNT(*) > 1
            """,
        },
        "unique_combination_viagens_remuneradas": {
            "query": """
            SELECT
                {expression}
            FROM
                `rj-smtr`.`{dataset_id}`.`{table_id}`
            WHERE
                DATA BETWEEN DATE("{start_timestamp}")
                AND DATE("{end_timestamp}")
            GROUP BY
                {expression}
            HAVING
                COUNT(*) > 1
            """,
        },
        "teste_completude": {
            "query": """
            WITH
                time_array AS (
                SELECT
                    *
                FROM
                    UNNEST(GENERATE_DATE_ARRAY(DATE("{start_timestamp}"), DATE("{end_timestamp}"))) AS DATA ),
                {table_id} AS (
                SELECT
                    DATA,
                    COUNT(*) AS q_registros
                FROM
                    `rj-smtr`.`{dataset_id2}`.`{table_id}`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    1 )
            SELECT
                DATA,
                q_registros
            FROM
                time_array
            LEFT JOIN
                {table_id}
            USING
                (DATA)
            WHERE
                q_registros IS NULL
                OR q_registros = 0
            """,
            "order_columns": ["DATA"],
        },
        "teste_completude_viagens_remuneradas": {
            "query": """
            WITH
                time_array AS (
                SELECT
                    *
                FROM
                    UNNEST(GENERATE_DATE_ARRAY(DATE("{start_timestamp}"), DATE("{end_timestamp}"))) AS DATA ),
                {table_id} AS (
                SELECT
                    DATA,
                    COUNT(*) AS q_registros
                FROM
                    `rj-smtr`.`{dataset_id}`.`{table_id}`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")
                GROUP BY
                    1 )
            SELECT
                DATA,
                q_registros
            FROM
                time_array
            LEFT JOIN
                {table_id}
            USING
                (DATA)
            WHERE
                q_registros IS NULL
                OR q_registros = 0
            """,
            "order_columns": ["DATA"],
        },
        "teste_sumario_servico_dia_tipo_soma_km": {
            "query": """
            WITH
                kms AS (
                SELECT
                    * EXCEPT({column_id}),
                    {column_id},
                    ROUND(COALESCE(km_apurada_registrado_com_ar_inoperante,0) + COALESCE(km_apurada_n_licenciado,0) + COALESCE(km_apurada_autuado_ar_inoperante,0) + COALESCE(km_apurada_autuado_seguranca,0) + COALESCE(km_apurada_autuado_limpezaequipamento,0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado,0) + COALESCE(km_apurada_licenciado_com_ar_n_autuado,0) + COALESCE(km_apurada_n_vistoriado, 0) + COALESCE(km_apurada_sem_transacao, 0),2) AS km_apurada2
                FROM
                    `rj-smtr`.`{dataset_id2}`.`{table_id2}`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}"))
            SELECT
                *,
                ABS(km_apurada2-{column_id}) AS dif
            FROM
                kms
            WHERE
                ABS(km_apurada2-{column_id}) > 0.02
            """,
            "order_columns": ["dif"],
        },
        "check_viagem_completa": {
            "query": """
            WITH
                data_versao_efetiva AS (
                SELECT
                    *
                FROM
                    rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva
                WHERE
                    DATA >= "2024-04-01"
                    AND DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")),
                viagem_completa AS (
                SELECT
                    *
                FROM
                    rj-smtr.projeto_subsidio_sppo.viagem_completa
                WHERE
                    DATA >= "2024-04-01"
                    AND DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")),
                feed_info AS (
                SELECT
                    *
                FROM
                    rj-smtr.gtfs.feed_info
                WHERE
                    feed_version IN (
                    SELECT
                    feed_version
                    FROM
                    data_versao_efetiva) )
                SELECT
                DISTINCT DATA
                FROM
                viagem_completa
                LEFT JOIN
                data_versao_efetiva AS d
                USING
                (DATA)
                LEFT JOIN
                feed_info AS i
                ON
                (DATA BETWEEN i.feed_start_date
                    AND i.feed_end_date
                    OR (DATA >= i.feed_start_date
                    AND i.feed_end_date IS NULL))
                WHERE
                i.feed_start_date != d.feed_start_date
                OR datetime_ultima_atualizacao < feed_update_datetime
            """,
            "order_columns": ["DATA"],
        },
        "teste_subsido_viagens_atualizadas": {
            "query": """
            WITH
                viagem_completa AS (
                SELECT
                    data,
                    datetime_ultima_atualizacao
                FROM
                    rj-smtr.projeto_subsidio_sppo.viagem_completa
                WHERE
                    DATA >= "2024-04-01"
                    AND DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}")),
                sumario_servico_dia_historico AS (
                SELECT
                    data,
                    datetime_ultima_atualizacao
                FROM
                    `rj-smtr`.`{dataset_id2}`.`{table_id3}`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}"))
                SELECT
                DISTINCT DATA
                FROM
                viagem_completa as c
                LEFT JOIN
                sumario_servico_dia_historico AS h
                USING
                (DATA)
                WHERE
                c.datetime_ultima_atualizacao > h.datetime_ultima_atualizacao
            """,
            "order_columns": ["DATA"],
        },
    }
    SUBSIDIO_SPPO_DATA_CHECKS_PRE_LIST = {
        "general": {
            "Todos os dados de GPS foram capturados": {
                "test": "check_gps_capture",
                "params": {
                    "interval": 1,
                    "dataset_id": smtr_constants.GPS_SPPO_RAW_DATASET_ID.value,
                    "table_id": smtr_constants.GPS_SPPO_RAW_TABLE_ID.value,
                },
            },
            "Todos os dados de realocação foram capturados": {
                "test": "check_gps_capture",
                "params": {
                    "interval": 10,
                    "dataset_id": smtr_constants.GPS_SPPO_RAW_DATASET_ID.value,
                    "table_id": smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
                },
            },
            "Todos os dados de GPS foram devidamente tratados": {
                "test": "check_gps_treatment",
            },
            "Todos os dados de status dos veículos foram devidamente tratados": {
                "test": "check_sppo_veiculo_dia",
            },
        }
    }
    SUBSIDIO_SPPO_DATA_CHECKS_POS_LIST = {
        "sumario_servico_dia": {
            "Todas as datas possuem dados": {"test": "teste_completude"},
            "Todos serviços com valores de penalidade aceitos": {
                "test": "accepted_values_valor_penalidade"
            },
            "Todos serviços abaixo do teto de pagamento de valor do subsídio": {
                "test": "teto_pagamento_valor_subsidio_pago",
                "expression": "ROUND(valor_subsidio_pago/subsidio_km_teto,2) <= ROUND(km_apurada+0.01,2)",
            },
            "Todos serviços são únicos em cada data": {
                "test": "unique_combination",
                "expression": "data, servico",
            },
            "Todos serviços possuem data não nula": {
                "expression": "data IS NOT NULL",
            },
            "Todos serviços possuem tipo de dia não nulo": {
                "expression": "tipo_dia IS NOT NULL",
            },
            "Todos serviços possuem consórcio não nulo": {
                "expression": "consorcio IS NOT NULL",
            },
            "Todas as datas possuem serviço não nulo": {
                "expression": "servico IS NOT NULL",
            },
            "Todos serviços com quantidade de viagens não nula e maior ou igual a zero": {
                "expression": "viagens IS NOT NULL AND viagens >= 0",
            },
            "Todos serviços com quilometragem apurada não nula e maior ou igual a zero": {
                "expression": "km_apurada IS NOT NULL AND km_apurada >= 0",
            },
            "Todos serviços com quilometragem planejada não nula e maior ou igual a zero": {
                "expression": "km_planejada IS NOT NULL AND km_planejada >= 0",
            },
            "Todos serviços com Percentual de Operação Diário (POD) não nulo e maior ou igual a zero": {
                "expression": "perc_km_planejada IS NOT NULL AND perc_km_planejada >= 0",
            },
            "Todos serviços com valor de subsídio pago não nulo e maior ou igual a zero": {
                "expression": "valor_subsidio_pago IS NOT NULL AND valor_subsidio_pago >= 0",
            },
        },
        "sumario_servico_dia_tipo_sem_glosa": {
            "Todas as somas dos tipos de quilometragem são equivalentes a quilometragem total": {
                "test": "teste_sumario_servico_dia_tipo_soma_km"
            },
            "Todas as datas possuem dados": {"test": "teste_completude"},
            "Todos serviços abaixo do teto de pagamento de valor do subsídio": {
                "test": "teto_pagamento_valor_subsidio_pago",
                "expression": "ROUND(valor_total_subsidio/subsidio_km_teto,2) <= ROUND(distancia_total_subsidio+0.01,2)",
            },
            "Todos serviços são únicos em cada data": {
                "test": "unique_combination",
                "expression": "data, servico",
            },
            "Todos serviços possuem data não nula": {
                "expression": "data IS NOT NULL",
            },
            "Todos serviços possuem tipo de dia não nulo": {
                "expression": "tipo_dia IS NOT NULL",
            },
            "Todos serviços possuem consórcio não nulo": {
                "expression": "consorcio IS NOT NULL",
            },
            "Todas as datas possuem serviço não nulo": {
                "expression": "servico IS NOT NULL",
            },
            "Todos serviços com quantidade de viagens não nula e maior ou igual a zero": {
                "expression": "viagens_subsidio IS NOT NULL AND viagens_subsidio >= 0",
            },
            "Todos serviços com quilometragem apurada não nula e maior ou igual a zero": {
                "expression": "distancia_total_subsidio IS NOT NULL AND distancia_total_subsidio >= 0",
            },
            "Todos serviços com quilometragem planejada não nula e maior ou igual a zero": {
                "expression": "distancia_total_planejada IS NOT NULL AND distancia_total_planejada >= 0",
            },
            "Todos serviços com Percentual de Operação Diário (POD) não nulo e maior ou igual a zero": {
                "expression": "perc_distancia_total_subsidio IS NOT NULL AND perc_distancia_total_subsidio >= 0",
            },
            "Todos serviços com valor total de subsídio não nulo e maior ou igual a zero": {
                "expression": "valor_total_subsidio IS NOT NULL AND valor_total_subsidio >= 0",
            },
            "Todos serviços com viagens por veículos não licenciados não nulo e maior ou igual a zero": {
                "expression": "viagens_n_licenciado IS NOT NULL AND viagens_n_licenciado >= 0",
            },
            "Todos serviços com quilometragem apurada por veículos não licenciados não nulo e maior ou igual a zero": {
                "expression": "km_apurada_n_licenciado IS NOT NULL AND km_apurada_n_licenciado >= 0",
            },
            "Todos serviços com viagens por veículos autuados por ar condicionado inoperante não nulo e maior ou igual a zero": {
                "expression": "viagens_autuado_ar_inoperante IS NOT NULL AND viagens_autuado_ar_inoperante >= 0",
            },
            "Todos serviços com quilometragem apurada por veículos autuados por ar condicionado inoperante não nulo e maior ou igual a zero": {
                "expression": "km_apurada_autuado_ar_inoperante IS NOT NULL AND km_apurada_autuado_ar_inoperante >= 0",
            },
            "Todos serviços com viagens por veículos autuados por segurança não nulo e maior ou igual a zero": {
                "expression": "viagens_autuado_seguranca IS NOT NULL AND viagens_autuado_seguranca >= 0",
            },
            "Todos serviços com quilometragem apurada por veículos autuados por segurança não nulo e maior ou igual a zero": {
                "expression": "km_apurada_autuado_seguranca IS NOT NULL AND km_apurada_autuado_seguranca >= 0",
            },
            "Todos serviços com viagens por veículos autuados por limpeza/equipamento não nulo e maior ou igual a zero": {
                "expression": "viagens_autuado_limpezaequipamento IS NOT NULL AND viagens_autuado_limpezaequipamento >= 0",
            },
            "Todos serviços com quilometragem apurada por veículos autuados por limpeza/equipamento não nulo e maior ou igual a zero": {
                "expression": "km_apurada_autuado_limpezaequipamento IS NOT NULL AND km_apurada_autuado_limpezaequipamento >= 0",
            },
            "Todos serviços com viagens por veículos sem ar condicionado e não autuado não nulo e maior ou igual a zero": {
                "expression": "viagens_licenciado_sem_ar_n_autuado IS NOT NULL AND viagens_licenciado_sem_ar_n_autuado >= 0",
            },
            "Todos serviços com quilometragem apurada por veículos sem ar condicionado e não autuado não nulo e maior ou igual a zero": {
                "expression": "km_apurada_licenciado_sem_ar_n_autuado IS NOT NULL AND km_apurada_licenciado_sem_ar_n_autuado >= 0",
            },
            "Todos serviços com viagens por veículos com ar condicionado e não autuado não nulo e maior ou igual a zero": {
                "expression": "viagens_licenciado_com_ar_n_autuado IS NOT NULL AND viagens_licenciado_com_ar_n_autuado >= 0",
            },
            "Todos serviços com quilometragem apurada por veículos com ar condicionado e não autuado não nulo e maior ou igual a zero": {
                "expression": "km_apurada_licenciado_com_ar_n_autuado IS NOT NULL AND km_apurada_licenciado_com_ar_n_autuado >= 0",
            },
            "Todos serviços com viagens por veículos registrados com ar condicionado inoperante não nulo e maior ou igual a zero": {
                "expression": "viagens_registrado_com_ar_inoperante IS NOT NULL AND viagens_registrado_com_ar_inoperante >= 0",
            },
            "Todos serviços com quilometragem apurada por veículos registrados com ar condicionado inoperante não nulo e maior ou igual a zero": {
                "expression": "km_apurada_registrado_com_ar_inoperante IS NOT NULL AND km_apurada_registrado_com_ar_inoperante >= 0",
            },
        },
        "viagens_remuneradas": {
            "Todas as datas possuem dados": {"test": "teste_completude_viagens_remuneradas"},
            "Todas viagens são únicas": {
                "test": "unique_combination_viagens_remuneradas",
                "expression": "id_viagem",
            },
            "Todas viagens possuem data": {
                "test": "expression_is_true_viagens_remuneradas",
                "expression": "data IS NOT NULL",
            },
            "Todas viagens possuem serviço não nulo": {
                "test": "expression_is_true_viagens_remuneradas",
                "expression": "servico IS NOT NULL",
            },
            "Todas viagens possuem ID não nulo": {
                "test": "expression_is_true_viagens_remuneradas",
                "expression": "id_viagem IS NOT NULL",
            },
            "Todas viagens possuem indicador de viagem remunerada não nulo e verdadeiro/falso": {
                "test": "expression_is_true_viagens_remuneradas",
                "expression": "indicador_viagem_dentro_limite IS NOT NULL\
                AND indicador_viagem_dentro_limite IN (TRUE, FALSE)",
            },
            "Todas viagens com distância planejada não nula e maior ou igual a zero": {
                "test": "expression_is_true_viagens_remuneradas",
                "expression": "distancia_planejada IS NOT NULL AND distancia_planejada >= 0",
            },
            "Todas viagens com valor de subsídio por km não nulo e maior ou igual a zero": {
                "test": "expression_is_true_viagens_remuneradas",
                "expression": "subsidio_km IS NOT NULL AND subsidio_km >= 0",
            },
            "Todas viagens atualizadas antes do processamento do subsídio": {
                "test": "teste_subsido_viagens_atualizadas",
            },
            "Todas viagens processadas com feed atualizado do GTFS": {
                "test": "check_viagem_completa",
            },
        },
        "sumario_servico_dia_pagamento": {
            "Todas as datas possuem dados": {"test": "teste_completude"},
            "Todos serviços com valores de penalidade aceitos": {
                "test": "accepted_values_valor_penalidade"
            },
            "Todos serviços abaixo do teto de pagamento de valor do subsídio": {
                "test": "teto_pagamento_valor_subsidio_pago",
                "expression": "ROUND((valor_a_pagar - valor_penalidade)/subsidio_km_teto,2) <= ROUND(km_apurada_dia+0.01,2)",
            },
            "Todos serviços são únicos em cada data": {
                "test": "unique_combination",
                "expression": "data, servico",
            },
            "Todos serviços possuem data não nula": {
                "expression": "data IS NOT NULL",
            },
            "Todos serviços possuem tipo de dia não nulo": {
                "expression": "tipo_dia IS NOT NULL",
            },
            "Todos serviços possuem consórcio não nulo": {
                "expression": "consorcio IS NOT NULL",
            },
            "Todas as datas possuem serviço não nulo": {
                "expression": "servico IS NOT NULL",
            },
            "Todos serviços com quantidade de viagens não nula e maior ou igual a zero": {
                "expression": "viagens_dia IS NOT NULL AND viagens_dia >= 0",
            },
            "Todos serviços com quilometragem apurada não nula e maior ou igual a zero": {
                "expression": "km_apurada_dia IS NOT NULL AND km_apurada_dia >= 0",
            },
            "Todos serviços com quilometragem planejada não nula e maior ou igual a zero": {
                "expression": "km_planejada_dia IS NOT NULL AND km_planejada_dia >= 0",
            },
            "Todos serviços com Percentual de Operação por Faixa Horária (POF) não nulo e maior ou igual a zero": {
                "expression": "media_pof IS NOT NULL AND media_pof >= 0",
            },
            "Todos serviços com valor de subsídio pago não nulo e maior ou igual a zero": {
                "expression": "(valor_a_pagar - valor_penalidade) IS NOT NULL AND (valor_a_pagar - valor_penalidade) >= 0",
            },
            "Todas as somas dos tipos de quilometragem são equivalentes a quilometragem total": {
                "test": "teste_sumario_servico_dia_tipo_soma_km"
            },
        },
    }
