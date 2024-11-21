# -*- coding: utf-8 -*-
"""Flows de captura dos dados da Jaé"""
from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.tasks import create_jae_general_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_TRANSACAO_ORDEM = create_default_capture_flow(
    flow_name="jae: transacao_ordem - captura",
    source=constants.TRANSACAO_ORDEM_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
set_default_parameters(CAPTURA_TRANSACAO_ORDEM, {"recapture": True})
