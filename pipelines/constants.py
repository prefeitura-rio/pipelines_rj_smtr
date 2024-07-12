# -*- coding: utf-8 -*-
"""
Valores constantes gerais para pipelines da rj-smtr

DBT 2024-07-03
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
    VEICULO_DATASET_ID = "veiculo"

    # STU

    STU_DATASET_ID = "br_rj_riodejaneiro_stu"

    STU_BUCKET_NAME = "rj-smtr-stu-private"

    # SUBSÍDIO RECURSOS

    SUBSIDIO_SPPO_RECURSO_TABLE_CAPTURE_PARAMS = {
        "recursos_sppo_viagens_individuais": "Viagem Individual",
        "recursos_sppo_bloqueio_via": "Bloqueio da via",
        "recursos_sppo_reprocessamento": "Reprocessamento",
    }

    SUBSIDIO_SPPO_RECURSOS_DATASET_ID = "br_rj_riodejaneiro_recursos"
    SUBSIDIO_SPPO_RECURSO_API_BASE_URL = "https://api.movidesk.com/public/v1/tickets"
    SUBSIDIO_SPPO_RECURSO_API_SECRET_PATH = "sppo_subsidio_recursos_api"

    # RDO/RHO
    RDO_FTPS_SECRET_PATH = "smtr_rdo_ftps"

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
            "max_pages": 70,
        },
        "primary_key": ["id"],
        "interval_minutes": 5,
    }

    # GTFS
    GTFS_CONTROLE_OS_URL = "https://docs.google.com/spreadsheets/d/e/2PAC\
X-1vRvFcyr9skfBIrjxc4FSJZ3-g4gUCF56YjGPOmd1f5qH8vndpy22P6f6KdcYgWaqzUmtSBL\
4Rs1Ardz/pub?gid=0&single=true&output=csv"
    GTFS_DATASET_ID = "br_rj_riodejaneiro_gtfs"

    GTFS_GENERAL_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "source_type": "gcs",
        "dataset_id": "br_rj_riodejaneiro_gtfs",
        "extract_params": {"filename": "gtfs"},
        "partition_date_name": "data_versao",
    }

    GTFS_TABLE_CAPTURE_PARAMS = {
        "ordem_servico": ["servico", "tipo_os"],
        "ordem_servico_trajeto_alternativo": ["servico", "tipo_os", "evento"],
        "shapes": ["shape_id", "shape_pt_sequence"],
        "agency": ["agency_id"],
        "calendar_dates": ["service_id", "date"],
        "calendar": ["service_id"],
        "feed_info": ["feed_publisher_name"],
        "frequencies": ["trip_id", "start_time"],
        "routes": ["route_id"],
        "stops": ["stop_id"],
        "trips": ["trip_id"],
        "fare_attributes": ["fare_id"],
        "fare_rules": [],
        "stop_times": ["trip_id", "stop_sequence"],
    }

    GTFS_MATERIALIZACAO_DATASET_ID = "gtfs"
    GTFS_MATERIALIZACAO_PARAMS = {
        "dataset_id": GTFS_DATASET_ID,
        "dbt_vars": {
            "data_versao_gtfs": "",
            "version": {},
        },
    }
    OS_COLUMNS = [
    "Serviço",
    "Vista",
    "Consórcio",
    "Extensão de Ida",
    "Extensão de Volta",
    "Horário Inicial Dia Útil",
    "Horário Fim Dia Útil",
    "Partidas Ida Dia Útil",
    "Partidas Volta Dia Útil",
    "Viagens Dia Útil",
    "Quilometragem Dia Útil",
    "Partidas Ida Sábado",
    "Partidas Volta Sábado",
    "Viagens Sábado",
    "Quilometragem Sábado",
    "Partidas Ida Domingo",
    "Partidas Volta Domingo",
    "Viagens Domingo",
    "Quilometragem Domingo",
    "Partidas Ida Ponto Facultativo",
    "Partidas Volta Ponto Facultativo",
    "Viagens Ponto Facultativo",
    "Quilometragem Ponto Facultativo"
    ]

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

    # SUBSÍDIO DASHBOARD
    # flake8: noqa: E501

    ZIRIX_API_SECRET_PATH = "zirix_api"
    VIAGEM_ZIRIX_RAW_DATASET_ID = "br_rj_riodejaneiro_viagem_zirix"
    ZIRIX_BASE_URL = "https://integration.systemsatx.com.br/Globalbus/SMTR"

    CONTROLE_FINANCEIRO_DATASET_ID = "controle_financeiro"

    ######################################
    # Discord code owners constants
    ######################################
    EMD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"
    DEFAULT_CODE_OWNERS = ["pipeliners"]
    OWNERS_DISCORD_MENTIONS = {
        # Register all code owners, users_id and type
        #     - possible types: https://docs.discord.club/embedg/reference/mentions
        #     - how to discover user_id: https://www.remote.tools/remote-work/how-to-find-discord-id
        #     - types: user, user_nickname, channel, role
        "pipeliners": {
            "user_id": "962067746651275304",
            "type": "role",
        },
        # "gabriel": {
        #     "user_id": "218800040137719809",
        #     "type": "user_nickname",
        # },
        "diego": {
            "user_id": "272581753829326849",
            "type": "user_nickname",
        },
        "joao": {
            "user_id": "692742616416256019",
            "type": "user_nickname",
        },
        "fernanda": {
            "user_id": "692709168221650954",
            "type": "user_nickname",
        },
        "paty": {
            "user_id": "821121576455634955",
            "type": "user_nickname",
        },
        "bruno": {
            "user_id": "183691546942636033",
            "type": "user_nickname",
        },
        "caio": {
            "user_id": "276427674002522112",
            "type": "user_nickname",
        },
        "anderson": {
            "user_id": "553786261677015040",
            "type": "user_nickname",
        },
        "rodrigo": {
            "user_id": "1031636163804545094",
            "type": "user_nickname",
        },
        "boris": {
            "user_id": "1109195532884262934",
            "type": "user_nickname",
        },
        "thiago": {
            "user_id": "404716070088343552",
            "type": "user_nickname",
        },
        "andre": {
            "user_id": "369657115012366336",
            "type": "user_nickname",
        },
        "rafaelpinheiro": {
            "user_id": "1131538976101109772",
            "type": "user_nickname",
        },
        "carolinagomes": {
            "user_id": "620000269392019469",
            "type": "user_nickname",
        },
        "karinappassos": {
            "user_id": "222842688117014528",
            "type": "user_nickname",
        },
        "danilo": {
            "user_id": "1147152438487416873",
            "type": "user_nickname",
        },
        "dados_smtr": {
            "user_id": "1056928259700445245",
            "type": "role",
        },
    }
