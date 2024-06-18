# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_stu
"""

from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.br_rj_riodejaneiro_stu.constants import constants
from pipelines.br_rj_riodejaneiro_stu.tasks import (
    create_final_stu_dataframe,
    get_stu_raw_blobs,
    read_stu_raw_file,
    save_stu_dataframes,
)
from pipelines.constants import constants as smtr_constants
from pipelines.utils.backup.flows import default_capture_flow
from pipelines.utils.backup.tasks import (
    get_current_flow_labels,
    get_current_timestamp,
    get_flow_project,
    rename_current_flow_run_now_time,
)
from pipelines.utils.backup.utils import set_default_parameters

stu_captura_subflow = deepcopy(default_capture_flow)
stu_captura_subflow.name = "SMTR: STU - Captura (subflow)"
stu_captura_subflow.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
stu_captura_subflow.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
stu_captura_subflow.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]

stu_captura_subflow = set_default_parameters(
    flow=stu_captura_subflow,
    default_parameters=constants.STU_GENERAL_CAPTURE_PARAMS.value,
)

with Flow(
    "SMTR: STU - Captura",
) as stu_captura:
    # SETUP
    data_versao_stu = Parameter("data_versao_stu", required=True)

    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=stu_captura.name + " ",
        now_time=timestamp,
    )

    LABELS = get_current_flow_labels()
    PROJECT = get_flow_project()

    # JOIN INDIVIDUAL FILES
    raw_files = get_stu_raw_blobs(data_versao_stu=data_versao_stu)

    raw_dfs = read_stu_raw_file.map(blob=raw_files)

    df_pf, df_pj = create_final_stu_dataframe(dfs=raw_dfs)

    SAVE_TABLE_FILES = save_stu_dataframes(df_pf=df_pf, df_pj=df_pj)

    # CAPTURE
    stu_capture_parameters = [
        {"timestamp": data_versao_stu, **d} for d in constants.STU_TABLE_CAPTURE_PARAMS.value
    ]

    run_captura = create_flow_run.map(
        flow_name=unmapped(stu_captura_subflow.name),
        project_name=unmapped(PROJECT),
        parameters=stu_capture_parameters,
        labels=unmapped(LABELS),
    )

    run_captura.set_upstream(SAVE_TABLE_FILES)

    wait_captura_true = wait_for_flow_run.map(
        run_captura,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

stu_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
stu_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
stu_captura.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
