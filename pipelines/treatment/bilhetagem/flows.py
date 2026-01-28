# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de bilhetagem

DBT: 2026-01-26
"""
from datetime import datetime, time, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pytz import timezone

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
        constants.INTEGRACAO_SELECTOR.value,
    ]
    + [
        s
        for s in jae_constants.JAE_AUXILIAR_SOURCES.value
        if s.table_id in ["gratuidade", "escola", "laudo_pcd", "estudante"]
    ],
    post_tests=constants.TRANSACAO_DAILY_TEST.value,
    test_webhook_key=jae_constants.ALERT_WEBHOOK.value,
    test_scheduled_time=time(12, 15, 0),
    skip_if_running_tolerance=10,
)

TRANSACAO_MATERIALIZACAO.state_handlers.append(handler_notify_failure(webhook="alertas_bilhetagem"))

INTEGRACAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="integracao - materializacao",
    selector=constants.INTEGRACAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cadastro_constants.CADASTRO_SELECTOR.value,
        jae_constants.INTEGRACAO_SOURCE.value,
    ]
    + [s for s in jae_constants.ORDEM_PAGAMENTO_SOURCES.value if s.table_id in ["ordem_rateio"]],
    test_webhook_key=jae_constants.ALERT_WEBHOOK.value,
    post_tests=constants.INTEGRACAO_DAILY_TEST.value,
)

INTEGRACAO_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)

INTEGRACAO_MATERIALIZACAO.schedule = Schedule(
    INTEGRACAO_MATERIALIZACAO.schedule.clocks
    + [
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 12, 15, tzinfo=timezone(smtr_constants.TIMEZONE.value)
            ),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
            parameter_defaults={"fallback_run": True},
        ),
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 14, 15, tzinfo=timezone(smtr_constants.TIMEZONE.value)
            ),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
            parameter_defaults={"fallback_run": True},
        ),
    ]
)

PASSAGEIRO_HORA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="passageiro_hora - materializacao",
    selector=constants.PASSAGEIRO_HORA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[constants.TRANSACAO_SELECTOR.value],
    test_webhook_key=jae_constants.ALERT_WEBHOOK.value,
    post_tests=constants.PASSAGEIRO_HORA_DAILY_TEST.value,
    test_scheduled_time=time(0, 25, 0),
)


TRANSACAO_ORDEM_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="transacao_ordem - materializacao",
    selector=constants.TRANSACAO_ORDEM_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        financeiro_constants.FINANCEIRO_BILHETAGEM_SELECTOR.value,
        jae_constants.TRANSACAO_ORDEM_SOURCE.value,
    ],
)

TRANSACAO_ORDEM_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)

TRANSACAO_ORDEM_MATERIALIZACAO.schedule = Schedule(
    TRANSACAO_ORDEM_MATERIALIZACAO.schedule.clocks
    + [
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 13, 15, tzinfo=timezone(smtr_constants.TIMEZONE.value)
            ),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
            parameter_defaults={"fallback_run": True},
        ),
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 16, 15, tzinfo=timezone(smtr_constants.TIMEZONE.value)
            ),
            labels=[
                smtr_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
            parameter_defaults={"fallback_run": True},
        ),
    ]
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
    test_webhook_key=jae_constants.ALERT_WEBHOOK.value,
    post_tests=constants.TRANSACAO_VALOR_ORDEM_DAILY_TEST.value,
)

EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="extrato_cliente_cartao - materializacao",
    selector=constants.EXTRATO_CLIENTE_CARTAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cadastro_constants.CADASTRO_SELECTOR.value,
        jae_constants.LANCAMENTO_SOURCE.value,
    ],
    skip_if_running_tolerance=10,
)
