# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de bilhetagem

DBT: 2025-04-04
"""

from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.bilhetagem.constants import constants
from pipelines.treatment.cadastro.constants import constants as cadastro_constants
from pipelines.treatment.financeiro.constants import constants as financeiro_constants
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.utils.prefect import handler_notify_failure

TRANSACAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="transacao - materializacao",
    selector=constants.TRANSACAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cadastro_constants.CADASTRO_SELECTOR.value,
        jae_constants.TRANSACAO_SOURCE.value,
        jae_constants.TRANSACAO_RIOCARD_SOURCE.value,
    ]
    + [s for s in jae_constants.JAE_AUXILIAR_SOURCES.value if s.table_id in ["gratuidade"]],
)

TRANSACAO_MATERIALIZACAO.state_handlers.append(handler_notify_failure(webhook="alertas_bilhetagem"))

INTEGRACAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="integracao - materializacao",
    selector=constants.INTEGRACAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cadastro_constants.CADASTRO_SELECTOR.value,
        jae_constants.INTEGRACAO_SOURCE.value,
    ],
)

INTEGRACAO_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)

PASSAGEIRO_HORA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="passageiro_hora - materializacao",
    selector=constants.PASSAGEIRO_HORA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[constants.TRANSACAO_SELECTOR.value],
)

GPS_VALIDADOR_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps_validador - materializacao",
    selector=constants.GPS_VALIDADOR_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cadastro_constants.CADASTRO_SELECTOR.value,
        jae_constants.GPS_VALIDADOR_SOURCE.value,
    ],
)

GPS_VALIDADOR_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)

TRANSACAO_ORDEM_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="transacao_ordem - materializacao",
    selector=constants.TRANSACAO_ORDEM_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[financeiro_constants.FINANCEIRO_BILHETAGEM_SELECTOR.value],
)

TRANSACAO_ORDEM_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)

TRANSACAO_VALOR_ORDEM_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="transacao_valor_ordem - materializacao",
    selector=constants.TRANSACAO_VALOR_ORDEM_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        constants.TRANSACAO_ORDEM_SELECTOR.value,
        constants.TRANSACAO_SELECTOR.value,
        constants.INTEGRACAO_SELECTOR.value,
    ],
    generate_schedule=False,
)
