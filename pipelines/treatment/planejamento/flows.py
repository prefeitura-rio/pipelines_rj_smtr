# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de planejamento

DBT: 2026-01-08a
"""

from pipelines.constants import constants as smtr_constants
from pipelines.treatment.planejamento.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

PLANEJAMENTO_DIARIO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="planejamento_diario - materializacao",
    selector=constants.PLANEJAMENTO_DIARIO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)

MATRIZ_INTEGRACAO_SMTR_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="matriz_integracao_smtr - materializacao",
    selector=constants.MATRIZ_INTEGRACAO_SMTR_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    generate_schedule=False,
)
