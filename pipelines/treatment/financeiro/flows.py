# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados financeiros
"""

from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.cadastro.constants import constants as cadastro_constants
from pipelines.treatment.financeiro.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

FINANCEIRO_BILHETAGEM_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="financeiro_bilhetagem - materializacao",
    selector=constants.FINANCEIRO_BILHETAGEM_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[cadastro_constants.CADASTRO_SELECTOR.value]
    + jae_constants.ORDEM_PAGAMENTO_SOURCES.value.values(),
)
