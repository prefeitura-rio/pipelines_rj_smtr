# -*- coding: utf-8 -*-
"""Flows de captura dos dados do STU"""
from pipelines.capture.stu.constants import constants
from pipelines.capture.stu.tasks import create_stu_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_STU = create_default_capture_flow(
    flow_name="stu: tabelas - captura",
    source=constants.STU_SOURCES.value,
    create_extractor_task=create_stu_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
set_default_parameters(CAPTURA_STU, {"recapture": True})
