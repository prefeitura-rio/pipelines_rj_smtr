# -*- coding: utf-8 -*-
"""Capture flows for Jae"""
from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.tasks import create_extractor_jae
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import every_minute

# Transação

BILHETAGEM_JAE_TRANSACAO_CAPTURE = create_default_capture_flow(
    flow_name="Bilhetagem Jaé Transação - Captura",
    create_extractor_task=create_extractor_jae,
    default_params=constants.TRANSACAO_CAPTURE_PARAMS.value,
    agent_label=smtr_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
)

BILHETAGEM_JAE_TRANSACAO_CAPTURE.schedule = every_minute

# Tabelas Auxiliares

BILHETAGEM_CADASTRO_JAE_AUXILIAR_CAPTURE = create_default_capture_flow(
    flow_name="Bilhetagem/Cadastro Jaé Auxiliar - Captura (subflow)",
    create_extractor_task=create_extractor_jae,
    default_params=constants.AUXILIAR_GENERAL_CAPTURE_PARAMS.value,
    agent_label=smtr_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
)
