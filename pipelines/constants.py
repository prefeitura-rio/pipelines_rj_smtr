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

    GPS_STPL_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_STPL_RAW_DATASET_ID = "br_rj_riodejaneiro_stpl_gps"
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
    GPS_BRT_DATASET_ID = "br_rj_riodejaneiro_veiculos"
    GPS_BRT_RAW_DATASET_ID = "br_rj_riodejaneiro_brt_gps"
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
    VEICULO_DATASET_ID = "veiculo"

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

    STU_DATASET_ID = "br_rj_riodejaneiro_stu"

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
    RDO_DATASET_ID = "br_rj_riodejaneiro_rdo"
    SPPO_RDO_TABLE_ID = "rdo_registros_sppo"
    SPPO_RHO_TABLE_ID = "rho_registros_sppo"
    STPL_RDO_TABLE_ID = "rdo_registros_stpl"
    STPL_RHO_TABLE_ID = "rho_registros_stpl"
    RDO_MATERIALIZE_START_DATE = "2022-12-07"


    # ROCK IN RIO
    RIR_DATASET_ID = "dashboards"
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
    SUBSIDIO_SPPO_DATASET_ID = "projeto_subsidio_sppo"
    SUBSIDIO_SPPO_TABLE_ID = "viagem_completa"

    # SUBSÍDIO DASHBOARD
    SUBSIDIO_SPPO_DASHBOARD_DATASET_ID = "dashboard_subsidio_sppo"
    SUBSIDIO_SPPO_DASHBOARD_TABLE_ID = "sumario_servico_dia"
    BILHETAGEM_DATASET_ID = "bilhetagem"
    CADASTRO_DATASET_ID = "cadastro"

    # CAPTURA #

    # JAE

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
                    data_tracking BETWEEN '{start}'
                    AND '{end}'
            """,
        },
        "primary_key": ["id"],
        "interval_minutes": 1,
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
            "table_id": "grupo",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        GRUPO
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_GRUPO"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "grupo_linha",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        GRUPO_LINHA
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_GRUPO", "CD_LINHA"],
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "matriz_integracao",
            "partition_date_only": True,
            "extract_params": {
                "database": "tarifa_db",
                "query": """
                    SELECT
                        *
                    FROM
                        matriz_integracao
                    WHERE
                        dt_inclusao BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": [
                "cd_versao_matriz",
                "cd_integracao",
            ],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "operadora_transporte",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        OPERADORA_TRANSPORTE
                    WHERE
                        DT_INCLUSAO BETWEEN '{start}'
                        AND '{end}'
                """,
            },
            "primary_key": ["CD_OPERADORA_TRANSPORTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
        {
            "table_id": "pessoa_juridica",
            "partition_date_only": True,
            "extract_params": {
                "database": "principal_db",
                "query": """
                    SELECT
                        *
                    FROM
                        PESSOA_JURIDICA
                """,
            },
            "primary_key": ["CD_CLIENTE"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
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
                """,
            },
            "primary_key": ["CD_CONSORCIO", "CD_LINHA"],  # id column to nest data on
            "interval_minutes": BILHETAGEM_TRATAMENTO_INTERVAL,
        },
    ]

    # BILHETAGEM_MATERIALIZACAO_TRANSACAO_PARAMS = {
    #     "dataset_id": BILHETAGEM_DATASET_ID,
    #     "table_id": BILHETAGEM_TRANSACAO_CAPTURE_PARAMS["table_id"],
    #     "upstream": True,
    #     "dbt_vars": {
    #         "date_range": {
    #             "table_run_datetime_column_name": "datetime_transacao",
    #             "delay_hours": 1,
    #         },
    #         "version": {},
    #     },
    # }

    # BILHETAGEM_MATERIALIZACAO_ORDEM_PAGAMENTO_PARAMS = {
    #     "dataset_id": BILHETAGEM_DATASET_ID,
    #     "table_id": "ordem_pagamento",
    #     "upstream": True,
    #     "exclude": f"+{BILHETAGEM_MATERIALIZACAO_TRANSACAO_PARAMS['table_id']}",
    #     "dbt_vars": {
    #         "date_range": {
    #             "table_run_datetime_column_name": "data_ordem",
    #             "delay_hours": 0,
    #         },
    #         "version": {},
    #     },
    # }

    # BILHETAGEM_GENERAL_CAPTURE_DEFAULT_PARAMS = {
    #     "dataset_id": BILHETAGEM_DATASET_ID,
    #     "secret_path": BILHETAGEM_SECRET_PATH,
    #     "source_type": BILHETAGEM_GENERAL_CAPTURE_PARAMS["source_type"],
    # }

    # GTFS
    GTFS_DATASET_ID = "br_rj_riodejaneiro_gtfs"

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

    # SUBSÍDIO RECURSOS VIAGENS INDIVIDUAIS
    SUBSIDIO_SPPO_RECURSOS_DATASET_ID = "br_rj_riodejaneiro_recurso"
    SUBSIDIO_SPPO_RECURSO_API_BASE_URL = "https://api.movidesk.com/public/v1/tickets?"
    SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH = "sppo_subsidio_recursos_api"
    SUBSIDIO_SPPO_RECURSO_SERVICE = "serviceFull eq 'SPPO'"
    SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "table_id": "recurso_sppo",
        "dataset_id": SUBSIDIO_SPPO_RECURSOS_DATASET_ID,
        "extract_params": {
            "token": "",
            "$select": "id,protocol,createdDate",
            "$filter": "{dates} and serviceFull/any(serviceFull: {service})",
            "$expand": "customFieldValues,customFieldValues($expand=items)",
            "$orderby": "createdDate asc",
        },
        "interval_minutes": 1440,
        "source_type": "movidesk",
        "primary_key": ["protocol"],
    }

    SUBSIDIO_SPPO_RECURSOS_MATERIALIZACAO_PARAMS = {
        "dataset_id": SUBSIDIO_SPPO_RECURSOS_DATASET_ID,
        "table_id": SUBSIDIO_SPPO_RECURSO_CAPTURE_PARAMS["table_id"],
        "upstream": True,
        "dbt_vars": {
            "date_range": {
                "table_run_datetime_column_name": "data_recurso",
                "delay_hours": 0,
            },
            "version": {},
        },
    }

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
