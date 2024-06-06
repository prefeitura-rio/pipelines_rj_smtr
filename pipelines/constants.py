# -*- coding: utf-8 -*-
"""
Valores constantes gerais para pipelines da rj-smtr
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes gerais para pipelines da rj-smtr
    """

    # trigger cd

    # CONFIGS #
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
    GCS_FLOWS_BUCKET = "datario-public"
    # PROJECT_NAME = {"dev": "rj-smtr-dev", "prod": "rj-smtr"}
    # DEFAULT_BUCKET_NAME = {"dev": "br-rj-smtr-dev", "prod": "br-rj-smtr"}
    PROJECT_NAME = {"dev": "rj-smtr-dev", "prod": "rj-smtr-dev"}
    DEFAULT_BUCKET_NAME = {"dev": "br-rj-smtr-dev", "prod": "br-rj-smtr-dev"}
    FILE_MAX_SIZE = 20_000
    PREFECT_DEFAULT_PROJECT = "production"

    # AGENT LABELS #
    RJ_SMTR_AGENT_LABEL = "rj-smtr"
    RJ_SMTR_DEV_AGENT_LABEL = "rj-smtr-dev"

    # DEFAULT TIMEZONE #
    TIMEZONE = "America/Sao_Paulo"

    # WEBHOOK #
    CRITICAL_SECRET_PATH = "critical_webhook"
    WEBHOOKS_SECRET_PATH = "webhooks"
    DATAPLEX_WEBHOOK = "dataplex"

    # RETRY POLICY #
    MAX_TIMEOUT_SECONDS = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 10

    # REDIS DEFAULT KEYS #
    REDIS_LAST_CAPTURED_VALUE_KEY = "last_captured_value"
    REDIS_LAST_MATERIALIZATION_TS_KEY = "last_run_timestamp"

    # PATTERNS #
    FILENAME_PATTERN = "%Y-%m-%d-%H-%M-%S"
    MATERIALIZATION_LAST_RUN_PATTERN = "%Y-%m-%dT%H:%M:%S"
    SOURCE_DATASET_ID_PATTERN = "{source_name}_source"
    MODE_FULL = "full"
    MODE_INCR = "incr"
    FLOW_RUN_URL_PATTERN = "https://pipelines.dados.rio/smtr/flow-run/{run_id}"

    # URLS #
    REPO_URL = "https://api.github.com/repos/prefeitura-rio/pipelines_rj_smtr"
    DATAPLEX_URL = "https://console.cloud.google.com/dataplex/govern/quality"

    # GPS STPL #
    GPS_STPL_API_BASE_URL = "http://zn4.m2mcontrol.com.br/api/integracao/veiculos"
    GPS_STPL_API_SECRET_PATH = "stpl_api"

    GPS_STPL_DATASET_ID = "migracao_br_rj_riodejaneiro_veiculos"
    GPS_STPL_RAW_DATASET_ID = "migracao_br_rj_riodejaneiro_stpl_gps"
    GPS_STPL_RAW_TABLE_ID = "registros"
    GPS_STPL_TREATED_TABLE_ID = "gps_stpl"

    # GPS SPPO #
    GPS_SPPO_API_BASE_URL = (
        "http://ccomobility.com.br/WebServices/Binder/WSConecta/EnvioInformacoesIplan?"
    )
    GPS_SPPO_API_BASE_URL_V2 = "http://ccomobility.com.br/WebServices/Binder/wsconecta/EnvioIplan?"
    GPS_SPPO_API_SECRET_PATH = "sppo_api"
    GPS_SPPO_API_SECRET_PATH_V2 = "sppo_api_v2"

    GPS_SPPO_RAW_DATASET_ID = "br_rj_riodejaneiro_onibus_gps"
    GPS_SPPO_RAW_TABLE_ID = "registros"
    GPS_SPPO_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_SPPO_TREATED_TABLE_ID = "gps_sppo"
    GPS_SPPO_CAPTURE_DELAY_V1 = 1
    GPS_SPPO_CAPTURE_DELAY_V2 = 60
    GPS_SPPO_RECAPTURE_DELAY_V2 = 6
    GPS_SPPO_MATERIALIZE_DELAY_HOURS = 1

    # REALOCAÇÃO #
    GPS_SPPO_REALOCACAO_RAW_TABLE_ID = "realocacao"
    GPS_SPPO_REALOCACAO_TREATED_TABLE_ID = "realocacao"
    GPS_SPPO_REALOCACAO_SECRET_PATH = "realocacao_api"

    # GPS BRT #
    GPS_BRT_API_SECRET_PATH = "brt_api_v2"
    GPS_BRT_API_URL = "https://zn4.m2mcontrol.com.br/api/integracao/veiculos"
    GPS_BRT_DATASET_ID = "migracao_br_rj_riodejaneiro_veiculos"
    GPS_BRT_RAW_DATASET_ID = "migracao_br_rj_riodejaneiro_brt_gps"
    GPS_BRT_RAW_TABLE_ID = "registros"
    GPS_BRT_TREATED_TABLE_ID = "gps_brt"
    GPS_BRT_MAPPING_KEYS = {
        "codigo": "id_veiculo",
        "linha": "servico",
        "latitude": "latitude",
        "longitude": "longitude",
        "dataHora": "timestamp_gps",
        "velocidade": "velocidade",
        "sentido": "sentido",
        "trajeto": "vista",
        # "inicio_viagem": "timestamp_inicio_viagem",
    }
    GPS_BRT_MATERIALIZE_DELAY_HOURS = 0

    # VEICULO
    VEICULO_DATASET_ID = "migracao_veiculo"

    # AUTUAÇÕES - AGENTES DE VERÃO
    SPPO_REGISTRO_AGENTE_VERAO_COLUMNS = [
        "datetime_registro",
        "email",
        "id_veiculo",
        "servico",
        "link_foto",
        "validacao",
    ]

    SPPO_REGISTRO_AGENTE_VERAO_PARAMS = {
        "partition_date_only": True,
        "source_type": "api-csv",
        "dataset_id": VEICULO_DATASET_ID,
        "table_id": "sppo_registro_agente_verao",
        "extract_params": {"secret_path": "smtr_agentes_verao"},
        "pre_treatment_reader_args": {
            "skiprows": 2,
            "names": SPPO_REGISTRO_AGENTE_VERAO_COLUMNS,
        },
        "primary_key": ["datetime_registro", "email"],
    }

    # STU

    STU_DATASET_ID = "migracao_br_rj_riodejaneiro_stu"

    STU_BUCKET_NAME = "rj-smtr-stu-private"

    STU_MODE_MAPPING = {
        "1": "Táxi",
        "2": "Ônibus",
        "3": "Escolar",
        "4": "Complementar (cabritinho)",
        "6": "Fretamento",
        "7": "TEC",
        "8": "Van",
    }

    STU_TYPE_MAPPING = [
        "Autônomo",
        "Empresa",
        "Cooperativa",
        "Instituicao de Ensino",
        "Associações",
        "Autônomo Provisório",
        "Contrato Público",
        "Prestadora de Serviços",
    ]

    STU_GENERAL_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "source_type": "gcs",
        "dataset_id": STU_DATASET_ID,
        "save_bucket_name": STU_BUCKET_NAME,
    }

    STU_TABLE_CAPTURE_PARAMS = [
        {
            "table_id": "operadora_empresa",
            "primary_key": ["Perm_Autor"],
            "pre_treatment_reader_args": {"dtype": "object"},
        },
        {
            "table_id": "operadora_pessoa_fisica",
            "primary_key": ["Perm_Autor"],
            "pre_treatment_reader_args": {"dtype": "object"},
        },
    ]

    # SUBSÍDIO RECURSOS

    SUBSIDIO_SPPO_RECURSO_TABLE_CAPTURE_PARAMS = {
        "recursos_sppo_viagens_individuais": "Viagem Individual",
        "recursos_sppo_bloqueio_via": "Bloqueio da via",
        "recursos_sppo_reprocessamento": "Reprocessamento",
    }

    SUBSIDIO_SPPO_RECURSOS_DATASET_ID = "migracao_br_rj_riodejaneiro_recursos"
    SUBSIDIO_SPPO_RECURSO_API_BASE_URL = "https://api.movidesk.com/public/v1/tickets"
    SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH = "sppo_subsidio_recursos_api"
    SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "dataset_id": SUBSIDIO_SPPO_RECURSOS_DATASET_ID,
        "extract_params": {
            "token": "",
            "$select": "id,protocol,createdDate,lastUpdate",
            "$filter": "serviceFirstLevel eq '{service} - Recurso Viagens Subsídio' \
and (lastUpdate ge {start} and lastUpdate lt {end} or createdDate ge {start} \
and createdDate lt {end})",
            "$expand": "customFieldValues,customFieldValues($expand=items)",
            "$orderby": "createdDate asc",
        },
        "interval_minutes": 1440,
        "source_type": "movidesk",
        "primary_key": ["protocol"],
    }

    SUBSIDIO_SPPO_RECURSOS_TABLE_IDS = [
        {"table_id": "recursos_sppo_viagens_individuais"},
        {"table_id": "recursos_sppo_bloqueio_via"},
        {"table_id": "recursos_sppo_reprocessamento"},
    ]

    SUBSIDIO_SPPO_RECURSOS_MATERIALIZACAO_PARAMS = {
        "dataset_id": "br_rj_riodejaneiro_recursos",
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "datetime_recurso",
                "delay_hours": 0,
            },
            "version": {},
        },
    }

    DIRETORIO_MATERIALIZACAO_PARAMS = {
        "dataset_id": "cadastro",
        "upstream": True,
    }

    DIRETORIO_MATERIALIZACAO_TABLE_PARAMS = [
        {"table_id": "diretorio_consorcios"},
        {"table_id": "operadoras_contatos"},
    ]

    # RDO/RHO
    RDO_FTP_ALLOWED_PATHS = ["SPPO", "STPL"]
    RDO_FTPS_SECRET_PATH = "smtr_rdo_ftps"
    RDO_DATASET_ID = "migracao_br_rj_riodejaneiro_rdo"
    SPPO_RDO_TABLE_ID = "rdo_registros_sppo"
    SPPO_RHO_TABLE_ID = "rho_registros_sppo"
    STPL_RDO_TABLE_ID = "rdo_registros_stpl"
    STPL_RHO_TABLE_ID = "rho_registros_stpl"
    RDO_MATERIALIZE_START_DATE = "2022-12-07"

    # ROCK IN RIO
    RIR_DATASET_ID = "migracao_dashboards"
    RIR_TABLE_ID = "registros_ocr_rir"
    RIR_START_DATE = "2022-08-30 12:00:00"
    RIR_SECRET_PATH = "smtr_rir_ftp"
    RIR_OCR_PRIMARY_COLUMNS = {
        "CodCET": "codigo_cet",
        "Placa": "placa",
        "UF": "uf",
        "LOCAL": "local",
        "datahora": "datahora",
    }
    RIR_OCR_SECONDARY_COLUMNS = {
        "RiR": "flag_rir",
        "Apoio": "flag_apoio",
    }

    # SUBSÍDIO
    SUBSIDIO_SPPO_DATASET_ID = "migracao_projeto_subsidio_sppo"
    SUBSIDIO_SPPO_TABLE_ID = "viagem_completa"

    # SUBSÍDIO DASHBOARD
    # BILHETAGEM_DATASET_ID = "migracao_bilhetagem"
    CADASTRO_DATASET_ID = "migracao_cadastro"

    # CAPTURA #

    # JAE - BILHETAGEM #

    BILHETAGEM_DATASET_ID = "br_rj_riodejaneiro_bilhetagem"

    BILHETAGEM_GENERAL_CAPTURE_PARAMS = {
        "databases": {
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
            "fiscalizacao_db": {
                "engine": "postgresql",
                "host": "10.5.115.29",
            },
        },
        "source_type": "db",
    }

    BILHETAGEM_TRACKING_CAPTURE_PARAMS = {
        "table_id": "gps_validador",
        "partition_date_only": False,
        "extract_params": {
            "database": "tracking_db",
            "query": """
                SELECT
                    *
                FROM
                    tracking_detalhe
                WHERE
                    id > {last_id} AND id <= {max_id}
            """,
            "page_size": 1000,
            "max_pages": 100,
        },
        "primary_key": ["id"],
        "interval_minutes": 5,
    }

    # GTFS
    GTFS_DATASET_ID = "migracao_br_rj_riodejaneiro_gtfs"

    GTFS_GENERAL_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "source_type": "gcs",
        "dataset_id": "br_rj_riodejaneiro_gtfs",
        "extract_params": {"filename": "gtfs"},
        "partition_date_name": "data_versao",
    }

    GTFS_TABLE_CAPTURE_PARAMS = [
        {
            "table_id": "shapes",
            "primary_key": ["shape_id", "shape_pt_sequence"],
        },
        {
            "table_id": "agency",
            "primary_key": ["agency_id"],
        },
        {
            "table_id": "calendar_dates",
            "primary_key": ["service_id", "date"],
        },
        {
            "table_id": "calendar",
            "primary_key": ["service_id"],
        },
        {
            "table_id": "feed_info",
            "primary_key": ["feed_publisher_name"],
        },
        {
            "table_id": "frequencies",
            "primary_key": ["trip_id", "start_time"],
        },
        {
            "table_id": "routes",
            "primary_key": ["route_id"],
        },
        {
            "table_id": "stops",
            "primary_key": ["stop_id"],
        },
        {
            "table_id": "trips",
            "primary_key": ["trip_id"],
        },
        {
            "table_id": "fare_attributes",
            "primary_key": ["fare_id"],
        },
        {
            "table_id": "fare_rules",
            "primary_key": [],
        },
        {
            "table_id": "ordem_servico",
            "primary_key": ["servico"],
            "extract_params": {"filename": "ordem_servico"},
        },
        {
            "table_id": "stop_times",
            "primary_key": ["trip_id", "stop_sequence"],
        },
    ]

    GTFS_MATERIALIZACAO_PARAMS = {
        "dataset_id": GTFS_DATASET_ID,
        "dbt_vars": {
            "data_versao_gtfs": "",
            "version": {},
        },
    }

    # # SUBSÍDIO RECURSOS VIAGENS INDIVIDUAIS
    # SUBSIDIO_SPPO_RECURSOS_DATASET_ID = "migracao_br_rj_riodejaneiro_recurso"
    # SUBSIDIO_SPPO_RECURSO_API_BASE_URL = "https://api.movidesk.com/public/v1/tickets?"
    # SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH = "sppo_subsidio_recursos_api"
    # SUBSIDIO_SPPO_RECURSO_SERVICE = "serviceFull eq 'SPPO'"
    # SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS = {
    #     "partition_date_only": True,
    #     "table_id": "recurso_sppo",
    #     "dataset_id": SUBSIDIO_SPPO_RECURSOS_DATASET_ID,
    #     "extract_params": {
    #         "token": "",
    #         "$select": "id,protocol,createdDate",
    #         "$filter": "{dates} and serviceFull/any(serviceFull: {service})",
    #         "$expand": "customFieldValues,customFieldValues($expand=items)",
    #         "$orderby": "createdDate asc",
    #     },
    #     "interval_minutes": 1440,
    #     "source_type": "movidesk",
    #     "primary_key": ["protocol"],
    # }

    # SUBSIDIO_SPPO_RECURSOS_MATERIALIZACAO_PARAMS = {
    #     "dataset_id": SUBSIDIO_SPPO_RECURSOS_DATASET_ID,
    #     "table_id": SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS["table_id"],
    #     "upstream": True,
    #     "dbt_vars": {
    #         "date_range": {
    #             "table_run_datetime_column_name": "data_recurso",
    #             "delay_hours": 0,
    #         },
    #         "version": {},
    #     },
    # }

    # VEÍCULOS LICENCIADOS
    # flake8: noqa: E501
    SPPO_LICENCIAMENTO_URL = (
        "https://siurblab.rio.rj.gov.br/SMTR/DADOS%20CADASTRAIS/Cadastro%20de%20Veiculos.txt"
    )
    SPPO_LICENCIAMENTO_MAPPING_KEYS = {
        "placa": "placa",
        "ordem": "id_veiculo",
        "permissao": "permissao",
        "modal": "modo",
        "ultima_vistoria": "data_ultima_vistoria",
        "cod_planta": "id_planta",
        "cod_mod_carroceria": "id_carroceria",
        "cod_fab_carroceria": "id_interno_carroceria",
        "des_mod_carroceria": "carroceria",
        "cod_mod_chassi": "id_chassi",
        "cod_fab_chassi": "id_fabricante_chassi",
        "des_mod_chassi": "nome_chassi",
        "lotacao_sentado": "quantidade_lotacao_sentado",
        "lotacao_pe": "quantidade_lotacao_pe",
        "elevador": "indicador_elevador",
        "ar_condicionado": "indicador_ar_condicionado_stu",
        "tipo_veiculo": "tipo_veiculo",
        "combustivel": "tipo_combustivel",
        "portas": "quantidade_portas",
        "ano_fabricacao": "ano_fabricacao",
        "wifi": "indicador_wifi",
        "usb": "indicador_usb",
        "data_inicio_vinculo": "data_inicio_vinculo",
    }
    SPPO_LICENCIAMENTO_CSV_ARGS = {
        "sep": ";",
        "names": SPPO_LICENCIAMENTO_MAPPING_KEYS.keys(),  # pylint: disable=e1101
    }
    SPPO_LICENCIAMENTO_TABLE_ID = "sppo_licenciamento_stu"

    # INFRAÇÃO
    SPPO_INFRACAO_URL = "https://siurblab.rio.rj.gov.br/SMTR/Multas/multas.txt"

    SPPO_INFRACAO_COLUMNS = [
        "permissao",
        "modo",
        "placa",
        "id_auto_infracao",
        "data_infracao",
        "valor",
        "id_infracao",
        "infracao",
        "status",
        "data_pagamento",
        "servico",
    ]

    SPPO_INFRACAO_MAPPING_KEYS = {
        "permissao": "permissao",
        "modal": "modo",
        "placa": "placa",
        "cm": "id_auto_infracao",
        "data_infracao": "data_infracao",
        "valor": "valor",
        "cod_infracao": "id_infracao",
        "des_infracao": "infracao",
        "status": "status",
        "data_pagamento": "data_pagamento",
        "linha": "servico",
    }
    SPPO_INFRACAO_CSV_ARGS = {
        "sep": ";",
        "names": SPPO_INFRACAO_MAPPING_KEYS.keys(),  # pylint: disable=e1101
    }
    SPPO_INFRACAO_TABLE_ID = "sppo_infracao"

    # VEÍCULO DIA
    SPPO_VEICULO_DIA_TABLE_ID = "sppo_veiculo_dia"

    # SUBSÍDIO DASHBOARD
    SUBSIDIO_SPPO_DASHBOARD_DATASET_ID = "migracao_dashboard_subsidio_sppo"
    SUBSIDIO_SPPO_DASHBOARD_STAGING_DATASET_ID = "migracao_dashboard_subsidio_sppo_staging"
    SUBSIDIO_SPPO_DASHBOARD_TABLE_ID = "sumario_servico_dia"
    SUBSIDIO_SPPO_DATA_CHECKS_PARAMS = {
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
                    `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros`
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
                    `rj-smtr`.`{dataset_id}`.`{table_id}`
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
                            `rj-smtr`.`{dataset_id}`.`{table_id}`
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
                    * EXCEPT(km_apurada),
                    km_apurada,
                    ROUND(COALESCE(km_apurada_registrado_com_ar_inoperante,0) + COALESCE(km_apurada_n_licenciado,0) + COALESCE(km_apurada_autuado_ar_inoperante,0) + COALESCE(km_apurada_autuado_seguranca,0) + COALESCE(km_apurada_autuado_limpezaequipamento,0) + COALESCE(km_apurada_licenciado_sem_ar_n_autuado,0) + COALESCE(km_apurada_licenciado_com_ar_n_autuado,0) + COALESCE(km_apurada_n_vistoriado, 0),2) AS km_apurada2
                FROM
                    `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_tipo`
                WHERE
                    DATA BETWEEN DATE("{start_timestamp}")
                    AND DATE("{end_timestamp}"))
            SELECT
                *,
                ABS(km_apurada2-km_apurada) AS dif
            FROM
                kms
            WHERE
                ABS(km_apurada2-km_apurada) > 0.02
            """,
            "order_columns": ["dif"],
        },
    }
    SUBSIDIO_SPPO_DATA_CHECKS_PRE_LIST = {
        "general": {
            "Todos os dados de GPS foram capturados": {
                "test": "check_gps_capture",
                "params": {
                    "interval": 1,
                    "dataset_id": GPS_SPPO_RAW_DATASET_ID,
                    "table_id": GPS_SPPO_RAW_TABLE_ID,
                },
            },
            "Todos os dados de realocação foram capturados": {
                "test": "check_gps_capture",
                "params": {
                    "interval": 10,
                    "dataset_id": GPS_SPPO_RAW_DATASET_ID,
                    "table_id": GPS_SPPO_REALOCACAO_RAW_TABLE_ID,
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
            "Todas as datas possuem dados": {"test": "teste_completude"},
            "Todas viagens são únicas": {
                "test": "unique_combination",
                "expression": "id_viagem",
            },
            "Todas viagens possuem data": {
                "expression": "data IS NOT NULL",
            },
            "Todas viagens possuem serviço não nulo": {
                "expression": "servico IS NOT NULL",
            },
            "Todas viagens possuem ID não nulo": {
                "expression": "id_viagem IS NOT NULL",
            },
            "Todas viagens possuem indicador de viagem remunerada não nulo e verdadeiro/falso": {
                "expression": "indicador_viagem_remunerada IS NOT NULL\
AND indicador_viagem_remunerada IN (TRUE, FALSE)",
            },
            "Todas viagens com distância planejada não nula e maior ou igual a zero": {
                "expression": "distancia_planejada IS NOT NULL AND distancia_planejada >= 0",
            },
            "Todas viagens com valor de subsídio por km não nulo e maior ou igual a zero": {
                "expression": "subsidio_km IS NOT NULL AND subsidio_km >= 0",
            },
        },
    }
