# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos
"""


from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants as smtr_constants
from pipelines.controle_financeiro.constants import constants
from pipelines.controle_financeiro.tasks import (
    cct_arquivo_retorno_save_redis,
    create_cct_arquivo_retorno_params,
    get_cct_arquivo_retorno_redis_key,
    get_raw_cct_arquivo_retorno,
)
from pipelines.schedules import every_day, every_friday_seven_thirty
from pipelines.utils.backup.flows import default_capture_flow
from pipelines.utils.backup.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    get_current_flow_mode,
    get_current_timestamp,
    get_now_time,
    parse_timestamp_to_string,
    rename_current_flow_run_now_time,
    transform_raw_to_nested_structure,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
)
from pipelines.utils.backup.utils import set_default_parameters

# SMTR Imports #


# Flows #

controle_cct_cb_captura = deepcopy(default_capture_flow)
controle_cct_cb_captura.name = "SMTR: Controle Financeiro CB - Captura"
controle_cct_cb_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cb_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)

controle_cct_cb_captura = set_default_parameters(
    flow=controle_cct_cb_captura,
    default_parameters=constants.SHEETS_CAPTURE_DEFAULT_PARAMS.value
    | constants.SHEETS_CB_CAPTURE_PARAMS.value,
)

controle_cct_cb_captura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

controle_cct_cb_captura.schedule = every_day

controle_cct_cett_captura = deepcopy(default_capture_flow)
controle_cct_cett_captura.name = "SMTR: Controle Financeiro CETT - Captura"
controle_cct_cett_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cett_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)

controle_cct_cett_captura = set_default_parameters(
    flow=controle_cct_cett_captura,
    default_parameters=constants.SHEETS_CAPTURE_DEFAULT_PARAMS.value
    | constants.SHEETS_CETT_CAPTURE_PARAMS.value,
)

controle_cct_cett_captura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

controle_cct_cett_captura.schedule = every_day


with Flow(
    "SMTR: Controle Financeiro Arquivo Retorno - Captura",
) as arquivo_retorno_captura:
    start_date = Parameter("start_date", default=None)
    end_date = Parameter("end_date", default=None)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=f"SMTR: Captura {constants.ARQUIVO_RETORNO_TABLE_ID.value}: ",
        now_time=get_now_time(),
    )
    MODE = get_current_flow_mode()

    timestamp = get_current_timestamp()

    REDIS_KEY = get_cct_arquivo_retorno_redis_key(mode=MODE)

    headers, params = create_cct_arquivo_retorno_params(
        redis_key=REDIS_KEY,
        start_date=start_date,
        end_date=end_date,
    )

    partitions = create_date_hour_partition(
        timestamp,
        partition_date_only=True,
    )

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value,
        table_id=constants.ARQUIVO_RETORNO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    raw_filepath = get_raw_cct_arquivo_retorno(
        headers=headers,
        params=params,
        local_filepath=filepath,
    )

    error = upload_raw_data_to_gcs(
        error=None,
        raw_filepath=raw_filepath,
        table_id=constants.ARQUIVO_RETORNO_TABLE_ID.value,
        dataset_id=smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value,
        partitions=partitions,
    )

    error, staging_filepath = transform_raw_to_nested_structure(
        raw_filepath=raw_filepath,
        filepath=filepath,
        error=error,
        timestamp=timestamp,
        primary_key=["id"],
    )

    staging_upload = upload_staging_data_to_gcs(
        error=error,
        staging_filepath=staging_filepath,
        timestamp=timestamp,
        table_id=constants.ARQUIVO_RETORNO_TABLE_ID.value,
        dataset_id=smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value,
        partitions=partitions,
    )

    cct_arquivo_retorno_save_redis(
        redis_key=REDIS_KEY,
        raw_filepath=raw_filepath,
        upstream_tasks=[staging_upload],
    )

arquivo_retorno_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
arquivo_retorno_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)

arquivo_retorno_captura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
]

arquivo_retorno_captura.schedule = every_friday_seven_thirty
