# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de trânsito

DBT 2025-04-08
"""

from pipelines.constants import constants as smtr_constants
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.treatment.transito.constants import constants

TRANSITO_AUTUACAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="autuacao - materializacao",
    selector=constants.TRANSITO_AUTUACAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
