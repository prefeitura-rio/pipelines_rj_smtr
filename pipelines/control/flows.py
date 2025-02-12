# -*- coding: utf-8 -*-
"""Flows variados para controle"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.control.tasks import set_redis_keys

with Flow("redis: alterar valor de key") as flow_set_key_redis:
    keys = Parameter(name="keys")

    set_redis_keys(keys=keys)


flow_set_key_redis.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
flow_set_key_redis.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
flow_set_key_redis.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
