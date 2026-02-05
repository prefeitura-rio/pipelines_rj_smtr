# -*- coding: utf-8 -*-
"""Flows de captura dos dados da Jaé"""
from datetime import datetime, timedelta

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)
from pytz import timezone

from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.tasks import (
    create_capture_check_discord_message,
    create_database_error_discord_message,
    create_jae_general_extractor,
    create_non_filtered_discord_message,
    create_ressarcimento_db_extractor,
    get_capture_gaps,
    get_end_value_historic_table,
    get_jae_db_config,
    get_non_filtered_tables,
    get_raw_backup_billingpay,
    get_table_info,
    get_timestamps_historic_table,
    jae_capture_check_get_ts_range,
    rename_flow_run_backup_billingpay,
    rename_flow_run_jae_capture_check,
    set_redis_backup_billingpay,
    set_redis_historic_table,
    test_jae_databases_connections,
    upload_backup_billingpay,
)
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import create_hourly_cron, every_day_hour_five, every_hour
from pipelines.tasks import get_run_env, get_scheduled_timestamp, log_discord
from pipelines.utils.prefect import set_default_parameters

# Capturas minuto a minuto

CAPTURA_TRANSACAO = create_default_capture_flow(
    flow_name="jae: transacao - captura",
    source=constants.TRANSACAO_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
    get_raw_max_retries=0,
)

CAPTURA_TRANSACAO_RIOCARD = create_default_capture_flow(
    flow_name="jae: transacao_riocard - captura",
    source=constants.TRANSACAO_RIOCARD_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
    get_raw_max_retries=0,
)

CAPTURA_GPS_VALIDADOR = create_default_capture_flow(
    flow_name="jae: gps_validador - captura",
    source=constants.GPS_VALIDADOR_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
    get_raw_max_retries=0,
    generate_schedule=False,
)

CAPTURA_LANCAMENTO = create_default_capture_flow(
    flow_name="jae: lancamento - captura",
    source=constants.LANCAMENTO_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
    get_raw_max_retries=0,
)

# Capturas a cada 10 minutos

CAPTURA_TRANSACAO_RETIFICADA = create_default_capture_flow(
    flow_name="jae: transacao_retificada - captura",
    source=constants.TRANSACAO_RETIFICADA_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
    get_raw_max_retries=0,
)

# Capturas por hora

CAPTURA_AUXILIAR = create_default_capture_flow(
    flow_name="jae: auxiliares - captura",
    source=constants.JAE_AUXILIAR_SOURCES.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    get_raw_max_retries=0,
    generate_schedule=False,
)
set_default_parameters(CAPTURA_AUXILIAR, {"recapture": True})

# Capturas diárias

CAPTURA_INTEGRACAO = create_default_capture_flow(
    flow_name="jae: integracao - captura",
    source=constants.INTEGRACAO_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    get_raw_max_retries=0,
)
set_default_parameters(CAPTURA_INTEGRACAO, {"recapture": True})

CAPTURA_INTEGRACAO.schedule = Schedule(
    CAPTURA_INTEGRACAO.schedule.clocks
    + [
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 11, 30, 7, 0, tzinfo=timezone(smtr_constants.TIMEZONE.value)),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 10, 0, tzinfo=timezone(smtr_constants.TIMEZONE.value)
            ),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

CAPTURA_INTEGRACAO.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
    cpu_limit="1000m",
    memory_limit="4600Mi",
    cpu_request="500m",
    memory_request="1000Mi",
)

CAPTURA_ORDEM_PAGAMENTO = create_default_capture_flow(
    flow_name="jae: ordem_pagamento - captura",
    source=constants.ORDEM_PAGAMENTO_SOURCES.value,
    create_extractor_task=create_ressarcimento_db_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    get_raw_max_retries=0,
)
set_default_parameters(CAPTURA_ORDEM_PAGAMENTO, {"recapture": True})

CAPTURA_TRANSACAO_ORDEM = create_default_capture_flow(
    flow_name="jae: transacao_ordem - captura",
    source=constants.TRANSACAO_ORDEM_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    get_raw_max_retries=0,
)
set_default_parameters(CAPTURA_TRANSACAO_ORDEM, {"recapture": True})

CAPTURA_TRANSACAO_ORDEM.schedule = Schedule(
    CAPTURA_TRANSACAO_ORDEM.schedule.clocks
    + [
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 12, 0, tzinfo=timezone(smtr_constants.TIMEZONE.value)
            ),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 14, 0, tzinfo=timezone(smtr_constants.TIMEZONE.value)
            ),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

# Flows de controle para captura.

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
    end_datetime = Parameter(name="end_datetime", default=None)

    env = get_run_env()

    database_config = get_jae_db_config(database_name=database_name)

    timestamp = get_scheduled_timestamp(timestamp=end_datetime)

    rename_flow_run_backup_billingpay(database_name=database_name, timestamp=timestamp)

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
        database_name=unmapped(database_name),
        timestamp=unmapped(timestamp),
    )

backup_billingpay.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
backup_billingpay.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
backup_billingpay.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

multiple_execution_dbs = ["processador_transacao_db", "financeiro_db", "midia_db"]

backup_billingpay.schedule = Schedule(
    [
        IntervalClock(
            interval=timedelta(hours=6) if db in multiple_execution_dbs else timedelta(days=1),
            start_date=datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone(smtr_constants.TIMEZONE.value))
            + timedelta(minutes=30 * idx),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
            parameter_defaults={"database_name": db},
        )
        for idx, db in enumerate(constants.BACKUP_JAE_BILLING_PAY.value.keys())
    ]
)

with Flow("jae: backup historico BillingPay") as backup_billingpay_historico:

    database_name = Parameter(name="database_name")
    table_id = Parameter(name="table_id")

    env = get_run_env()

    database_config = get_jae_db_config(database_name=database_name)

    timestamp = get_scheduled_timestamp()

    rename_flow_run_backup_billingpay(database_name=database_name, timestamp=timestamp)

    table_info = get_table_info(
        env=env,
        database_name=database_name,
        database_config=database_config,
        timestamp=timestamp,
        table_id=table_id,
    )

    table_info = get_timestamps_historic_table(
        env=env,
        database_name=database_name,
        table_info=table_info,
    )

    table_info = get_end_value_historic_table(
        table_info=table_info,
        database_name=database_name,
        database_config=database_config,
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

    set_redis_historic_table.map(
        env=unmapped(env),
        table_info=table_info,
        database_name=unmapped(database_name),
    )


backup_billingpay_historico.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
backup_billingpay_historico.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
backup_billingpay_historico.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

# backup_billingpay_historico.schedule = Schedule(
#     [
#         IntervalClock(
#             interval=timedelta(minutes=30),
#             start_date=datetime(
#                 2021, 1, 1, 0, 0, 0, tzinfo=timezone(smtr_constants.TIMEZONE.value)
#             ),
#             labels=[
#                 smtr_constants.RJ_SMTR_AGENT_LABEL.value,
#             ],
#             parameter_defaults={"database_name": db, "table_id": t},
#         )
#         for db, v in constants.BACKUP_JAE_BILLING_PAY_HISTORIC.value.items()
#         for t in v.keys()
#     ]
# )

with Flow("jae: verificacao captura") as verifica_captura:
    timestamp_captura_start = Parameter(name="timestamp_captura_start", default=None)
    timestamp_captura_end = Parameter(name="timestamp_captura_end", default=None)
    table_ids = Parameter(
        name="table_ids",
        default=list(constants.CHECK_CAPTURE_PARAMS.value.keys()),
    )
    retroactive_days = Parameter(name="retroactive_days", default=1)

    timestamp = get_scheduled_timestamp()

    env = get_run_env()

    timestamp_captura_start, timestamp_captura_end = jae_capture_check_get_ts_range(
        timestamp=timestamp,
        retroactive_days=retroactive_days,
        timestamp_captura_start=timestamp_captura_start,
        timestamp_captura_end=timestamp_captura_end,
    )

    rename_flow_run_jae_capture_check(
        timestamp_captura_start=timestamp_captura_start,
        timestamp_captura_end=timestamp_captura_end,
    )

    timestamps = get_capture_gaps.map(
        env=unmapped(env),
        table_id=table_ids,
        timestamp_captura_start=unmapped(timestamp_captura_start),
        timestamp_captura_end=unmapped(timestamp_captura_end),
    )

    discord_messages = create_capture_check_discord_message.map(
        table_id=table_ids,
        timestamps=timestamps,
        timestamp_captura_start=unmapped(timestamp_captura_start),
        timestamp_captura_end=unmapped(timestamp_captura_end),
    )

    send_discord_message = log_discord.map(
        message=discord_messages,
        key=unmapped(constants.ALERT_WEBHOOK.value),
    )

verifica_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
verifica_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
    cpu_limit="1000m",
    memory_limit="4600Mi",
    cpu_request="500m",
    memory_request="1000Mi",
)
verifica_captura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

verifica_captura.schedule = every_day_hour_five
