# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de bilhetagem
"""

from pipelines.constants import constants as smtr_constants
from pipelines.treatment.bilhetagem.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

PLANEJAMENTO_DIARIO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="transacao_ordem - materializacao",
    selector=constants.TRANSACAO_ORDEM_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
