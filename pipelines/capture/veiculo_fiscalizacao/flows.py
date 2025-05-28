# -*- coding: utf-8 -*-
"""Flows de captura dos dados de fiscalização de veiculos"""
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.capture.veiculo_fiscalizacao.constants import constants
from pipelines.capture.veiculo_fiscalizacao.tasks import (
    create_veiculo_fiscalizacao_lacre_extractor,
)
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_VEICULO_LACRE = create_default_capture_flow(
    flow_name="veiculo_fiscalizacao: veiculo_fiscalizacao_lacre - captura",
    source=constants.VEICULO_LACRE_SOURCE.value,
    create_extractor_task=create_veiculo_fiscalizacao_lacre_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_days=7,
)
set_default_parameters(CAPTURA_VEICULO_LACRE, {"recapture": True})
