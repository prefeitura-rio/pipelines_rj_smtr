# -*- coding: utf-8 -*-
"""Flows para exportação das transações do BQ para o Postgres"""

from types import NoneType

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import ifelse
from prefect.tasks.core.constants import Constant
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants as smtr_constants
from pipelines.tasks import get_run_env, get_scheduled_timestamp

# upload_files_postgres,; get_export_blobs,; save_upload_timestamp_redis,
from pipelines.upload_transacao_cct.tasks import (
    export_data_from_bq_to_gcs,
    full_refresh_delete_all_files,
    get_start_datetime,
)
from pipelines.utils.prefect import TypedParameter

with Flow(name="cct: transacao_cct postgresql - upload") as upload_transacao_cct:

    full_refresh = TypedParameter(
        name="full_refresh",
        default=False,
        accepted_types=bool,
    )

    data_ordem_start = TypedParameter(
        name="data_ordem_start",
        default=None,
        accepted_types=(str, NoneType),
    )

    data_ordem_end = TypedParameter(
        name="data_ordem_end",
        default=None,
        accepted_types=(str, NoneType),
    )

    env = get_run_env()

    timestamp = get_scheduled_timestamp()

    start_datetime = get_start_datetime(
        env=env,
        full_refresh=full_refresh,
        data_ordem_start=data_ordem_start,
        data_ordem_end=data_ordem_end,
    )

    full_refresh_delete = ifelse(
        full_refresh.is_equal(True),
        full_refresh_delete_all_files(env=env),
        Constant(None, name="delete_all_false"),
    )

    export_bigquery = export_data_from_bq_to_gcs(
        env=env,
        timestamp=timestamp,
        start_datetime=start_datetime,
        full_refresh=full_refresh,
        data_ordem_start=data_ordem_start,
        data_ordem_end=data_ordem_end,
        upstream_tasks=[full_refresh_delete],
    )

    # blobs = get_export_blobs(env=env, upstream_tasks=[export_bigquery])

    # upload = upload_files_postgres(
    #     env=env,
    #     blobs=blobs,
    #     full_refresh=full_refresh,
    # )

    # save_upload_timestamp_redis(
    #     env=env,
    #     timestamp=timestamp,
    #     data_ordem_start=data_ordem_start,
    #     upstream_tasks=[upload],
    # )


upload_transacao_cct.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
upload_transacao_cct.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
upload_transacao_cct.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
]
