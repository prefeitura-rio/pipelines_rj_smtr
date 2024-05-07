# -*- coding: utf-8 -*-
"""
Flows for gtfs
"""
from copy import deepcopy
from datetime import timedelta

from prefect import Parameter, case, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow

# from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

# SMTR Imports #
from pipelines.constants import constants
from pipelines.constants import constants as emd_constants
from pipelines.utils.backup.flows import (
    default_capture_flow,
    default_materialization_flow,
)
from pipelines.utils.backup.tasks import (
    get_current_flow_labels,
    get_current_timestamp,
    get_flow_project,
    get_scheduled_start_times,
    rename_current_flow_run_now_time,
)
from pipelines.utils.backup.utils import set_default_parameters

# from pipelines.capture.templates.flows import create_default_capture_flow


# Imports #


# EMD Imports #


# from pipelines.rj_smtr.flows import default_capture_flow, default_materialization_flow

# SETUP dos Flows

# gtfs_captura = create_default_capture_flow(
#     flow_name="SMTR: GTFS - Captura (subflow)",
#     agent_label=emd_constants.RJ_SMTR_AGENT_LABEL.value,
#     overwrite_flow_params=constants.GTFS_GENERAL_CAPTURE_PARAMS.value,
# )
gtfs_captura = deepcopy(default_capture_flow)
gtfs_captura.name = "SMTR: GTFS - Captura (subflow)"
gtfs_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
gtfs_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
gtfs_captura.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
gtfs_captura = set_default_parameters(
    flow=gtfs_captura,
    default_parameters=constants.GTFS_GENERAL_CAPTURE_PARAMS.value,
)

# gtfs_materializacao = create_default_materialization_flow(
#     flow_name="SMTR: GTFS - Materialização (subflow)",
#     agent_label=emd_constants.RJ_SMTR_AGENT_LABEL.value,
#     overwrite_flow_param_values=constants.GTFS_MATERIALIZACAO_PARAMS.value,
# )

gtfs_materializacao = deepcopy(default_materialization_flow)
gtfs_materializacao.name = "SMTR: GTFS - Materialização (subflow)"
gtfs_materializacao.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
gtfs_materializacao.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
gtfs_materializacao.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
gtfs_materializacao = set_default_parameters(
    flow=gtfs_materializacao,
    default_parameters=constants.GTFS_MATERIALIZACAO_PARAMS.value,
)

with Flow(
    "SMTR: GTFS - Captura/Tratamento",
    # code_owners=["rodrigo", "carolinagomes"],
) as gtfs_captura_tratamento:
    # SETUP
    data_versao_gtfs = Parameter("data_versao_gtfs", default=None)
    capture = Parameter("capture", default=True)
    materialize = Parameter("materialize", default=True)

    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=gtfs_captura_tratamento.name + " " + data_versao_gtfs + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()
    PROJECT = get_flow_project()

    with case(capture, True):
        gtfs_capture_parameters = [
            {"timestamp": data_versao_gtfs, **d} for d in constants.GTFS_TABLE_CAPTURE_PARAMS.value
        ]

        run_captura = create_flow_run.map(
            flow_name=unmapped(gtfs_captura.name),
            project_name=unmapped(PROJECT),
            parameters=gtfs_capture_parameters,
            labels=unmapped(LABELS),
            scheduled_start_time=get_scheduled_start_times(
                timestamp=timestamp,
                parameters=gtfs_capture_parameters,
                intervals={"agency": timedelta(minutes=11)},
            ),
        )

        wait_captura_true = wait_for_flow_run.map(
            run_captura,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )

    with case(capture, False):
        wait_captura_false = task(
            lambda: [None], checkpoint=False, name="assign_none_to_previous_runs"
        )()

    wait_captura = merge(wait_captura_true, wait_captura_false)

    with case(materialize, True):
        gtfs_materializacao_parameters = {
            "dbt_vars": {
                "data_versao_gtfs": data_versao_gtfs,
                "version": {},
            },
        }
        gtfs_materializacao_parameters_new = {
            "dataset_id": "gtfs",
            "dbt_vars": {
                "data_versao_gtfs": data_versao_gtfs,
                "version": {},
            },
        }

        run_materializacao = create_flow_run(
            flow_name=gtfs_materializacao.name,
            project_name=PROJECT,
            parameters=gtfs_materializacao_parameters,
            labels=LABELS,
            upstream_tasks=[wait_captura],
        )

        run_materializacao_new_dataset_id = create_flow_run(
            flow_name=gtfs_materializacao.name,
            project_name=PROJECT,
            parameters=gtfs_materializacao_parameters_new,
            labels=LABELS,
            upstream_tasks=[wait_captura],
        )

        wait_materializacao = wait_for_flow_run(
            run_materializacao,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        wait_materializacao_new_dataset_id = wait_for_flow_run(
            run_materializacao_new_dataset_id,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

gtfs_captura_tratamento.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
gtfs_captura_tratamento.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
gtfs_captura_tratamento.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
