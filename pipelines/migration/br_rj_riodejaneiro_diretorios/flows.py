# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_diretorios
"""

from copy import deepcopy

from prefect import Parameter, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow

# from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

# SMTR Imports #
from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_diretorios.constants import constants

# from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.migration.flows import default_materialization_flow
from pipelines.migration.tasks import (
    get_current_flow_labels,
    get_flow_project,
    get_rounded_timestamp,
    rename_current_flow_run_now_time,
)
from pipelines.migration.utils import set_default_parameters

# EMD Imports #


diretorios_materializacao_subflow = deepcopy(default_materialization_flow)
diretorios_materializacao_subflow.name = "SMTR: Diretórios - Materialização (subflow)"

diretorios_materializacao_subflow.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
diretorios_materializacao_subflow.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
diretorios_materializacao_subflow.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]

diretorios_materializacao_subflow = set_default_parameters(
    flow=diretorios_materializacao_subflow,
    default_parameters=constants.DIRETORIO_MATERIALIZACAO_PARAMS.value,
)


with Flow(
    "SMTR: Diretórios - Materialização",
    # code_owners=["caio", "fernanda", "boris", "rodrigo", "rafaelpinheiro"],
) as diretorios_materializacao:
    # Configuração #

    exclude = Parameter("exclude", default=None)

    timestamp = get_rounded_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=diretorios_materializacao.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()
    PROJECT = get_flow_project()

    table_params = task(
        lambda tables, exclude: (
            [t for t in tables if t["table_id"] not in exclude] if exclude is not None else tables
        ),
        checkpoint=False,
        name="get_tables_to_run",
    )(tables=constants.DIRETORIO_MATERIALIZACAO_TABLE_PARAMS.value, exclude=exclude)

    run_materializacao = create_flow_run.map(
        flow_name=unmapped(diretorios_materializacao_subflow.name),
        project_name=unmapped(PROJECT),
        labels=unmapped(LABELS),
        parameters=table_params,
    )

    wait_materializacao = wait_for_flow_run.map(
        run_materializacao,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )


diretorios_materializacao.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
diretorios_materializacao.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
diretorios_materializacao.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]
