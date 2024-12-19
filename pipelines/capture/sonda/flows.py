# -*- coding: utf-8 -*-
"""Flows de captura dos dados da SONDA"""
from pipelines.capture.sonda.constants import constants
from pipelines.capture.sonda.tasks import create_viagem_informada_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_VIAGEM_INFORMADA_BRT = create_default_capture_flow(
    flow_name="sonda: viagem_informada - captura",
    source=constants.VIAGEM_INFORMADA_SOURCE.value,
    create_extractor_task=create_viagem_informada_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
set_default_parameters(CAPTURA_VIAGEM_INFORMADA_BRT, {"recapture": True})
