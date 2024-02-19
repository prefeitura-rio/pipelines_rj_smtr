# -*- coding: utf-8 -*-
"""Flows de tratamento da bilhetagem"""
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.capture.jae.constants import constants as jae_capture_constants
from pipelines.capture.jae.flows import JAE_AUXILIAR_CAPTURE
from pipelines.constants import constants

# from pipelines.tasks import run_subflow


with Flow("Bilhetagem - Tratamento") as bilhetagem_tratamento:

    # AUXILIAR_CAPTURE = run_subflow(
    #     flow_name=JAE_AUXILIAR_CAPTURE.name,
    #     parameters=jae_capture_constants.AUXILIAR_TABLE_CAPTURE_PARAMS.value,
    #     maximum_parallelism=3,
    # )

    # AUXILIAR_CAPTURE.name = "run_captura_auxiliar_jae"

    runs_captura = create_flow_run.map(
        flow_name=JAE_AUXILIAR_CAPTURE.name,
        project_name="staging",
        parameters=jae_capture_constants.AUXILIAR_TABLE_CAPTURE_PARAMS.value[0],
        labels=[constants.RJ_SMTR_AGENT_LABEL.value],
    )

    wait_captura_true = wait_for_flow_run.map(
        runs_captura,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )


bilhetagem_tratamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bilhetagem_tratamento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMTR_AGENT_LABEL.value],
)
