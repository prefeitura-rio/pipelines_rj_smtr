# -*- coding: utf-8 -*-
"""Flows de captura dos tickets do MoviDesk"""
from pipelines.capture.movidesk.constants import constants
from pipelines.capture.movidesk.tasks import create_tickets_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_TICKETS = create_default_capture_flow(
    flow_name="movidesk: tickets - captura",
    source=constants.TICKETS_SOURCE.value,
    create_extractor_task=create_tickets_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
set_default_parameters(CAPTURA_TICKETS, {"recapture": True})
