# -*- coding: utf-8 -*-
"""Flows de captura dos dados de fiscalização de veiculos"""
from pipelines.capture.fiscalizacao_veiculo.constants import constants
from pipelines.capture.fiscalizacao_veiculo.tasks import create_veiculo_lacre_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_VEICULO_LACRE = create_default_capture_flow(
    flow_name=f"{constants.FISCALIZACAO_VEICULO_SOURCE_NAMEvalue}: {constants.VEICULO_LACRE_TABLE_ID.value} - captura",
    source=constants.VEICULO_LACRE_SOURCE.value,
    create_extractor_task=create_veiculo_lacre_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_days=7,
)
set_default_parameters(CAPTURA_VEICULO_LACRE, {"recapture": True})
