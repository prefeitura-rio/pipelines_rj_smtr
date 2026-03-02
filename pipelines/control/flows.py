# -*- coding: utf-8 -*-
"""Flows variados para controle

DBT 2026-02-02
"""
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.control.tasks import (
    parse_source_freshness_output,
    set_redis_keys,
    source_freshness_notify_discord,
)
from pipelines.schedules import every_hour
from pipelines.treatment.templates.tasks import run_dbt

with Flow("redis: alterar valor de key") as flow_set_key_redis:
    keys = Parameter(name="keys")

    set_redis_keys(keys=keys)


flow_set_key_redis.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
flow_set_key_redis.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
flow_set_key_redis.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]


with Flow("dbt: teste source freshness") as flow_source_freshness:

    dbt_output = run_dbt(resource="source freshness")
    flag_notify_discord, failed_sources = parse_source_freshness_output(dbt_output=dbt_output)
    with case(flag_notify_discord, True):
        discord_notification = source_freshness_notify_discord(failed_sources=failed_sources)

    flow_source_freshness.set_reference_tasks(
        [
            flag_notify_discord,
            failed_sources,
            discord_notification,
        ]
    )

flow_source_freshness.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
flow_source_freshness.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
flow_source_freshness.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
flow_source_freshness.schedule = every_hour
