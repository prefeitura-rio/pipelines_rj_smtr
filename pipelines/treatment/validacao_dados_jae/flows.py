# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados da validação dos dados da Jaé
"""

from pipelines.constants import constants as smtr_constants
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.treatment.validacao_dados_jae.constants import constants

PLANEJAMENTO_DIARIO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="validacao_dados_jae - materializacao",
    selector=constants.VALIDACAO_DADOS_JAE_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
