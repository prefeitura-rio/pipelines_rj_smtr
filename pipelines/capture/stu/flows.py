# -*- coding: utf-8 -*-
"""Capture flows for Stu"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_skip_if_running,
)
from pipelines.capture.stu.constants import constants as stu_capture_constant
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants
from pipelines.capture.stu.tasks import create_extractor_stu
from pipelines.tasks import run_subflow
from pipelines.utils.prefect import TypedParameter

STU_CAPTURE_SUBFLOW = create_default_capture_flow(
    flow_name="STU Operadores - Captura (subflow)",
    source_name=stu_capture_constant.STU_SOURCE_NAME.value,
    partition_date_only=False,
    create_extractor_task=create_extractor_stu,
    overwrite_flow_params=stu_capture_constant.STU_GENERAL_CAPTURE_PARAMS.value,
    agent_label=constants.RJ_SMTR_AGENT_LABEL.value,
)

with Flow("STU Operadores - Captura") as stu_capture:

    data_versao_stu = TypedParameter(
        name="data_versao_stu",
        accepted_types=str,
    )

    STU_CAPTURE_RUN = run_subflow(
        flow_name=STU_CAPTURE_SUBFLOW.name,
        parameters=[
            {**p, "data_extractor_params": {"data_versao_stu": data_versao_stu}}
            for p in stu_capture_constant.STU_CAPTURE_PARAMS.value
        ],
    )

    STU_CAPTURE_RUN.name = "run_captura_operadores"

stu_capture.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
stu_capture.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMTR_AGENT_LABEL.value],
)

stu_capture.state_handlers = [
    handler_inject_bd_credentials,
    handler_skip_if_running,
]
