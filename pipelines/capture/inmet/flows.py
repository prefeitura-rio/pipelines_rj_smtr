# -*- coding: utf-8 -*-
"""
Flows de captura dos dados do INMET

DBT: 2025-09-29
"""
from pipelines.capture.inmet.constants import constants
from pipelines.capture.inmet.tasks import create_temperatura_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants

CAPTURA_TEMPERATURA_INMET = create_default_capture_flow(
    flow_name="inmet: temperatura - captura",
    source=constants.INMET_METEOROLOGIA_SOURCE.value,
    create_extractor_task=create_temperatura_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    generate_schedule=False,
)
