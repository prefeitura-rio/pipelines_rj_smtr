# -*- coding: utf-8 -*-
"""Capture flows for Jae"""
from datetime import timedelta

from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.tasks import create_extractor_jae
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import generate_schedule

# Transação

JAE_TRANSACAO_CAPTURE = create_default_capture_flow(
    flow_name="Jaé Transação - Captura",
    source_name=constants.JAE_SOURCE_NAME.value,
    partition_date_only=False,
    create_extractor_task=create_extractor_jae,
    overwrite_flow_params=constants.TRANSACAO_DEFAULT_PARAMS.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)

JAE_TRANSACAO_CAPTURE.schedule = generate_schedule(
    interval=timedelta(minutes=5),
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)

# Tabelas Auxiliares

JAE_AUXILIAR_CAPTURE = create_default_capture_flow(
    flow_name="Jaé Auxiliar - Captura (subflow)",
    source_name=constants.JAE_SOURCE_NAME.value,
    partition_date_only=True,
    create_extractor_task=create_extractor_jae,
    overwrite_flow_params=constants.AUXILIAR_GENERAL_CAPTURE_PARAMS.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
