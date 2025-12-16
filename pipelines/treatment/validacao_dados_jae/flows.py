# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados da validação dos dados da Jaé

DBT 2025-11-12
"""

from pipelines.constants import constants as smtr_constants
from pipelines.treatment.bilhetagem.constants import constants as bilhetagem_constants
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.treatment.validacao_dados_jae.constants import constants
from pipelines.utils.prefect import handler_notify_failure

VALIDACAO_DADOS_JAE_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="validacao_dados_jae - materializacao",
    selector=constants.VALIDACAO_DADOS_JAE_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    post_tests=constants.VALIDACAO_DADOS_JAE_DAILY_TEST.value,
    wait=[
        bilhetagem_constants.TRANSACAO_SELECTOR.value,
        bilhetagem_constants.INTEGRACAO_SELECTOR.value,
    ],
)

VALIDACAO_DADOS_JAE_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)

ALERTA_TRANSACAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="alerta_transacao - materializacao",
    selector=constants.ALERTA_TRANSACAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    generate_schedule=False,
)
