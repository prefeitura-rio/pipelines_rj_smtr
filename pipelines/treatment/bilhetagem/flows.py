# -*- coding: utf-8 -*-
"""Flows de tratamento da bilhetagem"""
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.capture.jae.constants import constants as jae_capture_constants
from pipelines.capture.jae.flows import JAE_AUXILIAR_CAPTURE
from pipelines.constants import constants
from pipelines.tasks import run_subflow

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
