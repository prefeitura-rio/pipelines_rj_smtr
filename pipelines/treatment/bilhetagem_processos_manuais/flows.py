# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados financeiros

DBT 2025-10-14
"""
from types import NoneType

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import case, merge
from prefect.tasks.core.constants import Constant
from prefect.tasks.core.operators import NotEqual
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.capture.jae.flows import (
    CAPTURA_INTEGRACAO,
    CAPTURA_ORDEM_PAGAMENTO,
    CAPTURA_TRANSACAO_ORDEM,
    verifica_captura,
)
from pipelines.constants import constants as smtr_constants
from pipelines.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    parse_timestamp_to_string,
    run_subflow,
)
from pipelines.treatment.bilhetagem.flows import (
    INTEGRACAO_MATERIALIZACAO,
    TRANSACAO_ORDEM_MATERIALIZACAO,
)
from pipelines.treatment.bilhetagem_processos_manuais.constants import constants
from pipelines.treatment.bilhetagem_processos_manuais.tasks import (
    create_gap_materialization_params,
    create_transacao_ordem_integracao_capture_params,
    create_verify_capture_params,
    get_gaps_from_result_table,
)
from pipelines.treatment.financeiro.flows import (
    FINANCEIRO_BILHETAGEM_MATERIALIZACAO,
    ordem_pagamento_quality_check,
)
from pipelines.utils.prefect import TypedParameter

with Flow(name="financeiro_bilhetagem: ordem atrasada - captura/tratamento") as ordem_atrasada:
    timestamp = TypedParameter(
        name="timestamp",
        default=None,
        accepted_types=(NoneType, str),
    )

    timestamp = get_scheduled_timestamp(timestamp=timestamp)

    run_recapture = run_subflow(
        flow_name=CAPTURA_ORDEM_PAGAMENTO.name,
        parameters=[
            {
                "table_id": s.table_id,
                "recapture": True,
            }
            for s in jae_constants.ORDEM_PAGAMENTO_SOURCES.value
        ],
        maximum_parallelism=3,
    )

    run_capture = run_subflow(
        flow_name=CAPTURA_ORDEM_PAGAMENTO.name,
        parameters=[
            {
                "table_id": s.table_id,
                "timestamp": parse_timestamp_to_string(
                    timestamp=timestamp, pattern="%Y-%m-%d %H:%M:%S"
                ),
                "recapture": False,
            }
            for s in jae_constants.ORDEM_PAGAMENTO_SOURCES.value
        ],
        maximum_parallelism=3,
        upstream_tasks=[run_recapture],
    )

    run_materializacao_financeiro_bilhetagem = run_subflow(
        flow_name=FINANCEIRO_BILHETAGEM_MATERIALIZACAO.name, upstream_tasks=[run_capture]
    )

    run_ordem_quality_check = run_subflow(
        flow_name=ordem_pagamento_quality_check.name,
        upstream_tasks=[run_materializacao_financeiro_bilhetagem],
    )

    integracao_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.INTEGRACAO_TABLE_ID.value,
        upstream_tasks=[run_ordem_quality_check],
    )

    run_captura_integracao = run_subflow(
        flow_name=CAPTURA_INTEGRACAO.name,
        parameters=integracao_capture_params,
    )

    run_materializacao_integracao = run_subflow(
        flow_name=INTEGRACAO_MATERIALIZACAO.name,
        upstream_tasks=[run_captura_integracao],
    )

    transacao_ordem_capture_params = create_transacao_ordem_integracao_capture_params(
        timestamp=timestamp,
        table_id=jae_constants.TRANSACAO_ORDEM_TABLE_ID.value,
        upstream_tasks=[run_materializacao_integracao],
    )

    run_captura_transacao_ordem = run_subflow(
        flow_name=CAPTURA_TRANSACAO_ORDEM.name,
        parameters=transacao_ordem_capture_params,
    )

    run_materializacao_transacao_ordem = run_subflow(
        flow_name=TRANSACAO_ORDEM_MATERIALIZACAO.name,
        upstream_tasks=[run_captura_transacao_ordem],
    )


ordem_atrasada.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
ordem_atrasada.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
ordem_atrasada.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

with Flow(
    name="jae: timestamps divergentes - recaptura/tratamento"
) as timestamp_divergente_jae_recaptura:

    timestamp_start = TypedParameter(
        name="timestamp_start",
        default=None,
        accepted_types=(NoneType, str),
    )

    timestamp_end = TypedParameter(
        name="timestamp_end",
        default=None,
        accepted_types=(NoneType, str),
    )

    tables = constants.CAPTURE_GAP_TABLES.value

    env = get_run_env()

    gaps = get_gaps_from_result_table(
        env=env,
        table_ids=tables.keys(),
        timestamp_start=timestamp_start,
        timestamp_end=timestamp_end,
    )

    upstream_task = gaps
    for k, v in tables.items():

        with case(gaps[k]["flag_has_gaps"].is_equal(True), True):
            run_recapture_true = run_subflow(
                flow_name=v["flow_name"],
                parameters=gaps[k]["recapture_params"],
                upstream_tasks=[upstream_task],
            )

        with case(gaps[k]["flag_has_gaps"].is_equal(True), False):
            run_recapture_false = Constant(
                value=None,
                name="run_recapture_false",
            )

        run_recapture = merge(run_recapture_true, run_recapture_false)

        upstream_task = run_recapture

    materialization_params = create_gap_materialization_params(
        gaps=gaps, env=env, upstream_tasks=[upstream_task]
    )

    selectors = constants.CAPTURE_GAP_SELECTORS.value

    upstream_task = materialization_params
    for k, v in selectors.items():
        params = materialization_params[k]
        with case(params.is_not_equal(None), True):
            run_rematerialize_true = run_subflow(
                flow_name=v["flow_name"],
                parameters=params,
                upstream_tasks=[upstream_task],
            )

        with case(NotEqual().run(params, None), False):
            run_rematerialize_false = Constant(value=None, name="run_rematerialize_false")

        run_rematerialize = merge(run_rematerialize_true, run_rematerialize_false)
        upstream_task = run_rematerialize

    verify_capture_params = create_verify_capture_params(gaps=gaps, upstream_tasks=[upstream_task])

    run_subflow(
        flow_name=verifica_captura.name,
        parameters=verify_capture_params,
        upstream_tasks=[upstream_task],
    )

timestamp_divergente_jae_recaptura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
timestamp_divergente_jae_recaptura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
timestamp_divergente_jae_recaptura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
