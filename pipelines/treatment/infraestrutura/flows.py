# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de infraestrutura

DBT: 2025-11-17
"""

from pipelines.constants import constants as smtr_constants
from pipelines.treatment.infraestrutura.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

INFRAESTRUTURA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="infraestrutura - materializacao",
    selector=constants.INFRAESTRUTURA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
