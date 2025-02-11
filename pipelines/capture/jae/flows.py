# -*- coding: utf-8 -*-
"""Flows de captura dos dados da Jaé"""
from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.tasks import (
    create_database_error_discord_message,
    create_jae_general_extractor,
    create_non_filtered_discord_message,
    get_jae_db_config,
    get_non_filtered_tables,
    get_raw_backup_billingpay,
    get_table_info,
    set_redis_backup_billingpay,
    test_jae_databases_connections,
    upload_backup_billingpay,
)
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import every_hour
from pipelines.tasks import get_run_env, get_scheduled_timestamp, log_discord
from pipelines.utils.prefect import set_default_parameters

CAPTURA_TRANSACAO_ORDEM = create_default_capture_flow(
    flow_name="jae: transacao_ordem - captura",
    source=constants.TRANSACAO_ORDEM_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
set_default_parameters(CAPTURA_TRANSACAO_ORDEM, {"recapture": True})

with Flow("jae: verifica ip do banco de dados") as verificacao_ip:
    success, failed_connections = test_jae_databases_connections()
    with case(success, False):
        message = create_database_error_discord_message(failed_connections=failed_connections)
        send_discord_message = log_discord(
            message=message,
            key=constants.ALERT_WEBHOOK.value,
            dados_tag=True,
        )
    verificacao_ip.set_reference_tasks(tasks=[send_discord_message, success])

verificacao_ip.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
verificacao_ip.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
verificacao_ip.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
verificacao_ip.schedule = every_hour

with Flow("jae: backup dados BillingPay") as backup_billingpay:

    database_name = Parameter(name="database_name")

    env = get_run_env()

    database_config = get_jae_db_config(database_name=database_name)

    timestamp = get_scheduled_timestamp()

    table_info = get_table_info(
        env=env,
        database_name=database_name,
        database_config=database_config,
        timestamp=timestamp,
    )
    send_message, table_count = get_non_filtered_tables(
        database_name=database_name,
        database_config=database_config,
        table_info=table_info,
    )
    with case(send_message, True):
        message = create_non_filtered_discord_message(
            database_name=database_name,
            table_count=table_count,
        )

        send_discord_message = log_discord(
            message=message,
            key=constants.ALERT_WEBHOOK.value,
            dados_tag=True,
        )

    table_info = get_raw_backup_billingpay(
        table_info=table_info,
        database_config=database_config,
        timestamp=timestamp,
    )

    table_info = upload_backup_billingpay.map(
        env=unmapped(env),
        table_info=table_info,
        database_name=unmapped(database_name),
    )

    SET_REDIS_BACKUP = set_redis_backup_billingpay.map(
        env=unmapped(env),
        table_info=table_info,
        database_name=database_name,
        timestamp=unmapped(timestamp),
    )

verificacao_ip.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
verificacao_ip.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
verificacao_ip.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
