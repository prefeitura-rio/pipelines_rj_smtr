# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de planejamento

DBT 2024-12-19
"""

from pipelines.constants import constants as smtr_constants
from pipelines.treatment.planejamento.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

PLANEJAMENTO_DIARIO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="planejamento_diario - materializacao",
    selector=constants.PLANEJAMENTO_DIARIO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
