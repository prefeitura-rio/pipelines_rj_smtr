# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados financeiros

DBT 2025-09-02
"""
from types import NoneType

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import ifelse
from prefect.tasks.core.constants import Constant
from prefect.tasks.core.operators import Equal
from prefect.utilities.collections import DotDict
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.capture.jae.flows import CAPTURA_ORDEM_PAGAMENTO
from pipelines.constants import constants as smtr_constants
from pipelines.tasks import get_run_env, get_scheduled_timestamp, run_subflow
from pipelines.treatment.bilhetagem.flows import (
    INTEGRACAO_MATERIALIZACAO,
    TRANSACAO_ORDEM_MATERIALIZACAO,
)
from pipelines.treatment.bilhetagem_processos_manuais.constants import constants
from pipelines.treatment.bilhetagem_processos_manuais.tasks import (
    create_gap_materialization_params,
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

    run_capture = run_subflow(
        flow_name=CAPTURA_ORDEM_PAGAMENTO.name,
        parameters=[
            {"table_id": s.table_id, "timestamp": timestamp, "recapture": False}
            for s in jae_constants.ORDEM_PAGAMENTO_SOURCES.value
        ],
        maximum_parallelism=3,
    )

    run_materializacao_financeiro_bilhetagem = run_subflow(
        flow_name=FINANCEIRO_BILHETAGEM_MATERIALIZACAO.name, upstream_tasks=[run_capture]
    )

    run_ordem_quality_check = run_subflow(
        flow_name=ordem_pagamento_quality_check.name,
        upstream_tasks=[run_materializacao_financeiro_bilhetagem],
    )

    run_materializacao_transacao_ordem = run_subflow(
        flow_name=TRANSACAO_ORDEM_MATERIALIZACAO.name,
        upstream_tasks=[run_ordem_quality_check],
    )

    run_materializacao_integracao = run_subflow(
        flow_name=INTEGRACAO_MATERIALIZACAO.name,
        upstream_tasks=[run_materializacao_transacao_ordem],
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
    name="jae: timestamps com divergentes - recaptura/tratamento"
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

    upstream_tasks = None
    for k, v in tables.items():
        run_recapture = ifelse(
            gaps[k]["flag_has_gaps"].is_equal(True),
            run_subflow(
                flow_name=v,
                parameters={"recapture": True, "recapture_timestamps": gaps[k]["timestamps"]},
                upstream_tasks=upstream_tasks,
            ),
            Constant(value=None, name="run_recapture_false"),
        )
        upstream_tasks = [run_recapture]

    materialization_params = DotDict(
        create_gap_materialization_params(gaps=gaps, upstream_tasks=run_recapture)
    )

    selectors = constants.CAPTURE_GAP_SELECTORS.value
    upstream_tasks = None
    for k, v in selectors.items():
        params = materialization_params.get(k)
        run_rematerialize = ifelse(
            Equal().run(params, None),
            run_subflow(
                flow_name=v["flow_name"],
                parameters=params,
                upstream_tasks=upstream_tasks,
            ),
            Constant(value=None, name="run_rematerialize_false"),
        )
        upstream_tasks = [run_rematerialize]


timestamp_divergente_jae_recaptura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
timestamp_divergente_jae_recaptura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
timestamp_divergente_jae_recaptura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
