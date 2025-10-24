# -*- coding: utf-8 -*-
"""Flows para exportação das transações do BQ para o Postgres"""

# a
from types import NoneType

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import case, merge
from prefect.tasks.core.constants import Constant
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants as smtr_constants
from pipelines.schedules import every_day_hour_one
from pipelines.tasks import get_run_env, get_scheduled_timestamp
from pipelines.treatment.templates.tasks import (
    dbt_data_quality_checks,
    get_repo_version,
    run_dbt,
)
from pipelines.upload_transacao_cct.constants import constants
from pipelines.upload_transacao_cct.tasks import (
    delete_all_files,
    export_data_from_bq_to_gcs,
    get_postgres_modified_dates,
    get_start_datetime,
    save_upload_timestamp_redis,
    upload_files_postgres,
    upload_postgres_modified_data_to_bq,
)
from pipelines.utils.prefect import TypedParameter, handler_notify_failure

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

    param_test_dates = TypedParameter(
        name="test_dates",
        default=None,
        accepted_types=(list, NoneType),
    )

    env = get_run_env()

    timestamp = get_scheduled_timestamp()

    with case(param_test_dates, None):

        start_datetime, full_refresh_test_none = get_start_datetime(
            env=env,
            full_refresh=full_refresh,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
        )

        delete_files = delete_all_files(env=env)

        export_bigquery_dates = export_data_from_bq_to_gcs(
            env=env,
            timestamp=timestamp,
            start_datetime=start_datetime,
            full_refresh=full_refresh_test_none,
            data_ordem_start=data_ordem_start,
            data_ordem_end=data_ordem_end,
            upstream_tasks=[delete_files],
        )

        upload_postgres_test_none = upload_files_postgres(
            env=env,
            full_refresh=full_refresh_test_none,
            export_bigquery_dates=export_bigquery_dates,
        )

        test_dates_test_none = get_postgres_modified_dates(
            env=env, start_datetime=start_datetime, full_refresh=full_refresh
        )

    with case(param_test_dates.is_not_equal(None), True):
        upload_postgres_test_not_none = Constant(None, name="upload_postgres_test_not_none")
        test_dates_test_not_none = param_test_dates

    upload_postgres = merge(upload_postgres_test_not_none, upload_postgres_test_none)
    full_refresh = merge(full_refresh, full_refresh_test_none)
    test_dates = merge(test_dates_test_not_none, test_dates_test_none)

    upload_test_bq = upload_postgres_modified_data_to_bq(
        env=env,
        timestamp=timestamp,
        dates=test_dates,
        full_refresh=full_refresh,
        upstream_tasks=[upload_postgres],
    )

    run_sincronizacao_model = run_dbt(
        resource="model",
        model=constants.TESTE_SINCRONIZACAO_TABLE_NAME.value,
        _vars={"version": get_repo_version()},
        upstream_tasks=[upload_test_bq],
    )

    run_sincronizacao_test = run_dbt(
        resource="test",
        model=constants.TESTE_SINCRONIZACAO_TABLE_NAME.value,
        upstream_tasks=[run_sincronizacao_model],
    )

    notify_discord = dbt_data_quality_checks(
        dbt_logs=run_sincronizacao_test,
        checks_list=constants.SINCRONIZACAO_CHECKS_LIST.value,
        params={},
        webhook_key="alertas_bilhetagem",
    )

    save_redis_upload = save_upload_timestamp_redis(
        env=env,
        timestamp=timestamp,
        data_ordem_start=data_ordem_start,
        upstream_tasks=[upload_postgres_test_none, notify_discord],
    )


upload_transacao_cct.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
upload_transacao_cct.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
upload_transacao_cct.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
    handler_notify_failure(webhook="alertas_bilhetagem"),
]
upload_transacao_cct.schedule = every_day_hour_one
