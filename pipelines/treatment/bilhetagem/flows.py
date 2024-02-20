# -*- coding: utf-8 -*-
"""Flows de tratamento da bilhetagem"""
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.capture.jae.constants import constants as jae_capture_constants
from pipelines.capture.jae.flows import JAE_AUXILIAR_CAPTURE
from pipelines.constants import constants
from pipelines.tasks import run_subflow

# from pipelines.utils.prefect import handler_cancel_subflows

with Flow("Bilhetagem - Tratamento") as bilhetagem_tratamento:

    AUXILIAR_CAPTURE = run_subflow(
        flow_name=JAE_AUXILIAR_CAPTURE.name,
        parameters=jae_capture_constants.AUXILIAR_TABLE_CAPTURE_PARAMS.value,
        maximum_parallelism=3,
    )

    AUXILIAR_CAPTURE.name = "run_captura_auxiliar_jae"


bilhetagem_tratamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bilhetagem_tratamento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_tratamento.state_handlers = [
    handler_inject_bd_credentials,
    handler_skip_if_running,
    # handler_cancel_subflows,
]
