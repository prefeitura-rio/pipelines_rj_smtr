# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados financeiros

DBT 2025-09-02
"""
from types import NoneType

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.capture.jae.flows import CAPTURA_ORDEM_PAGAMENTO
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import every_day_hour_nine
from pipelines.tasks import get_run_env, get_scheduled_timestamp, run_subflow
from pipelines.treatment.bilhetagem.flows import (
    INTEGRACAO_MATERIALIZACAO,
    TRANSACAO_ORDEM_MATERIALIZACAO,
)
from pipelines.treatment.cadastro.constants import constants as cadastro_constants
from pipelines.treatment.financeiro.constants import constants
from pipelines.treatment.financeiro.tasks import (
    get_ordem_pagamento_modified_partitions,
    get_ordem_quality_check_end_datetime,
    get_ordem_quality_check_start_datetime,
    set_redis_quality_check_datetime,
)
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.treatment.templates.tasks import dbt_data_quality_checks, run_dbt
from pipelines.utils.prefect import TypedParameter, handler_notify_failure

FINANCEIRO_BILHETAGEM_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="financeiro_bilhetagem - materializacao",
    selector=constants.FINANCEIRO_BILHETAGEM_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cadastro_constants.CADASTRO_SELECTOR.value,
    ]
    + jae_constants.ORDEM_PAGAMENTO_SOURCES.value,
)

FINANCEIRO_BILHETAGEM_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)

with Flow(
    name="bilhetagem_consorcio_operador_dia - quality check"
) as ordem_pagamento_quality_check:
    start_datetime = TypedParameter(
        name="start_datetime",
        default=None,
        accepted_types=(NoneType, str),
    )
    end_datetime = TypedParameter(
        name="end_datetime",
        default=None,
        accepted_types=(NoneType, str),
    )
    partitions = TypedParameter(
        name="partitions",
        default=None,
        accepted_types=(NoneType, list),
    )
    DATASET_ID = constants.FINANCEIRO_DATASET_ID.value
    TABLE_ID = "bilhetagem_consorcio_operador_dia"

    env = get_run_env()
    timestamp = get_scheduled_timestamp()
    start_datetime = get_ordem_quality_check_start_datetime(
        env=env,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        start_datetime=start_datetime,
        partitions=partitions,
    )

    end_datetime = get_ordem_quality_check_end_datetime(
        timestamp=timestamp,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        partitions=partitions,
    )

    dbt_vars, test_name = get_ordem_pagamento_modified_partitions(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        partitions=partitions,
    )

    test_result = run_dbt(
        resource="test",
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        test_name=test_name,
        _vars=dbt_vars,
    )

    notify_discord = dbt_data_quality_checks(
        dbt_logs=test_result,
        checks_list=constants.ORDEM_PAGAMENTO_CHECKS_LIST.value,
        webhook_key="alertas_bilhetagem_ordem_pagamento",
        params=dbt_vars,
        raise_check_error=False,
        additional_mentions=["devs_smtr"],
    )

    set_redis = set_redis_quality_check_datetime(
        env=env,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        end_datetime=end_datetime,
        upstream_tasks=[notify_discord],
    )

    ordem_pagamento_quality_check.set_reference_tasks([set_redis, notify_discord])

ordem_pagamento_quality_check.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
ordem_pagamento_quality_check.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
ordem_pagamento_quality_check.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
ordem_pagamento_quality_check.schedule = every_day_hour_nine

with Flow(name="financeiro_bilhetagem - captura e tratamento de ordem atrasada") as ordem_atrasada:
    timestamp = TypedParameter(
        name="timestamp",
        default=None,
        accepted_types=(NoneType, str),
    )

    env = get_run_env()
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
