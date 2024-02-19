# -*- coding: utf-8 -*-
"""Flows de tratamento da bilhetagem"""

from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.capture.jae.constants import constants as jae_capture_constants
from pipelines.capture.jae.flows import JAE_AUXILIAR_CAPTURE
from pipelines.tasks import run_flow_mapped

with Flow("Bilhetagem - Tratamento") as bilhetagem_tratamento:
    AUXILIAR_CAPTURE = run_flow_mapped(
        flow_name=JAE_AUXILIAR_CAPTURE.name,
        parameters=jae_capture_constants.AUXILIAR_TABLE_CAPTURE_PARAMS.value,
        maximum_parallelism=3,
    )

    AUXILIAR_CAPTURE.name = "run_captura_auxiliar"
