# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_rdo

DBT 2024-09-06
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped

# from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.schedules import every_two_weeks
from prefeitura_rio.pipelines_utils.custom import Flow

# from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_rdo.constants import constants
from pipelines.migration.br_rj_riodejaneiro_rdo.tasks import (
    check_files_for_download,
    download_and_save_local_from_ftp,
    get_file_paths_from_ftp,
    get_rdo_date_range,
    pre_treatment_br_rj_riodejaneiro_rdo,
    update_redis_ftp_files,
)
from pipelines.migration.tasks import (
    bq_upload,
    fetch_dataset_sha,
    get_current_flow_labels,
    get_current_flow_mode,
    get_current_timestamp,
    get_flow_project,
    get_join_dict,
    get_now_time,
    rename_current_flow_run_now_time,
    run_dbt_model,
    set_last_run_timestamp,
)
from pipelines.schedules import every_day

# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client
# from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

with Flow(
    "SMTR: RDO - Materialização (subflow)",
    # code_owners=constants.DEFAULT_CODE_OWNERS.value,
) as sppo_rdo_materialize:
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=sppo_rdo_materialize.name + ": ", now_time=get_now_time()
    )

    # Get default parameters #
    dataset_id = Parameter("dataset_id", default=constants.RDO_DATASET_ID.value)
    table_id = Parameter("table_id", default=None)
    rebuild = Parameter("rebuild", False)

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # Set dbt client #
    # dbt_client = get_k8s_dbt_client(mode=MODE)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    date_range = get_rdo_date_range(
        dataset_id=dataset_id,
        table_id=constants.SPPO_RHO_TABLE_ID.value,
        mode=MODE,
    )
    version = fetch_dataset_sha(dataset_id=dataset_id)
    dbt_vars = get_join_dict([date_range], version)
    # Run materialization #
    with case(rebuild, True):
        RUN = run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=True,
            _vars=dbt_vars,
            flags="--full-refresh",
            exclude="+consorcios",
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=constants.SPPO_RHO_TABLE_ID.value,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )
    with case(rebuild, False):
        RUN = run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=True,
            _vars=dbt_vars,
            exclude="+consorcios",
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=constants.SPPO_RHO_TABLE_ID.value,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )

sppo_rdo_materialize.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
sppo_rdo_materialize.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_rdo_materialize.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]


with Flow(
    "SMTR: RHO - Captura (subflow)",
) as captura_sppo_rho:
    # SETUP
    transport_mode = Parameter("transport_mode", "SPPO")
    report_type = Parameter("report_type", "RHO")
    dump = Parameter("dump", False)
    table_id = Parameter("table_id", constants.SPPO_RHO_TABLE_ID.value)
    materialize = Parameter("materialize", False)

    rename_run = rename_current_flow_run_now_time(
        prefix=f"{captura_sppo_rho.name} FTP - {transport_mode.run()}-{report_type.run()} ",
        now_time=get_current_timestamp(),
        wait=None,
    )
    # EXTRACT
    files = get_file_paths_from_ftp(
        transport_mode=transport_mode, report_type=report_type, dump=dump
    )
    download_files = check_files_for_download(
        files=files, dataset_id=constants.RDO_DATASET_ID.value, table_id=table_id
    )
    updated_info = download_and_save_local_from_ftp.map(file_info=download_files)
    # TRANSFORM
    treated_path, raw_path, partitions, status = pre_treatment_br_rj_riodejaneiro_rdo(
        files=updated_info
    )
    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(constants.RDO_DATASET_ID.value),
        table_id=unmapped(table_id),
        filepath=treated_path,
        raw_filepath=raw_path,
        partitions=partitions,
        status=status,
    )
    set_redis = update_redis_ftp_files(
        download_files=download_files, table_id=table_id, errors=errors
    )

captura_sppo_rho.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_rho.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_sppo_rho.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

with Flow(
    "SMTR: RDO - Captura (subflow)",
) as captura_sppo_rdo:
    # SETUP
    transport_mode = Parameter("transport_mode", "SPPO")
    report_type = Parameter("report_type", "RDO")
    dump = Parameter("dump", False)
    table_id = Parameter("table_id", constants.SPPO_RDO_TABLE_ID.value)
    materialize = Parameter("materialize", False)

    rename_run = rename_current_flow_run_now_time(
        prefix=f"{captura_sppo_rdo.name} FTP - {transport_mode.run()}-{report_type.run()} ",
        now_time=get_current_timestamp(),
        wait=None,
    )
    # EXTRACT
    files = get_file_paths_from_ftp(
        transport_mode=transport_mode, report_type=report_type, dump=dump
    )
    download_files = check_files_for_download(
        files=files, dataset_id=constants.RDO_DATASET_ID.value, table_id=table_id
    )
    updated_info = download_and_save_local_from_ftp.map(file_info=download_files)
    # TRANSFORM
    treated_path, raw_path, partitions, status = pre_treatment_br_rj_riodejaneiro_rdo(
        files=updated_info
    )
    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(constants.RDO_DATASET_ID.value),
        table_id=unmapped(table_id),
        filepath=treated_path,
        raw_filepath=raw_path,
        partitions=partitions,
        status=status,
    )
    set_redis = update_redis_ftp_files(
        download_files=download_files, table_id=table_id, errors=errors
    )

captura_sppo_rdo.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_rdo.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_sppo_rdo.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

with Flow(
    "SMTR: RDO - Captura/Tratamento",
) as rdo_captura_tratamento:
    LABELS = get_current_flow_labels()
    PROJECT = get_flow_project()

    run_captura_rho_sppo = create_flow_run(
        flow_name=captura_sppo_rho.name,
        project_name=PROJECT,
        labels=LABELS,
    )

    wait_captura_rho_sppo = wait_for_flow_run(
        run_captura_rho_sppo,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    run_captura_rho_stpl = create_flow_run(
        flow_name=captura_sppo_rho.name,
        project_name=PROJECT,
        labels=LABELS,
        parameters={
            "transport_mode": "STPL",
            "table_id": constants.STPL_RHO_TABLE_ID.value,
        },
    )

    wait_captura_rho_stpl = wait_for_flow_run(
        run_captura_rho_stpl,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    run_captura_rdo_sppo = create_flow_run(
        flow_name=captura_sppo_rdo.name,
        project_name=PROJECT,
        labels=LABELS,
        upstream_tasks=[wait_captura_rho_sppo, run_captura_rho_stpl],
    )

    wait_captura_rdo_sppo = wait_for_flow_run(
        run_captura_rdo_sppo,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    run_captura_rdo_stpl = create_flow_run(
        flow_name=captura_sppo_rdo.name,
        project_name=PROJECT,
        labels=LABELS,
        parameters={
            "transport_mode": "STPL",
            "table_id": constants.STPL_RHO_TABLE_ID.value,
        },
        upstream_tasks=[wait_captura_rho_sppo, run_captura_rho_stpl],
    )

    wait_captura_rdo_stpl = wait_for_flow_run(
        run_captura_rdo_stpl,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    run_materializacao = create_flow_run(
        flow_name=sppo_rdo_materialize.name,
        project_name=PROJECT,
        labels=LABELS,
        upstream_tasks=[wait_captura_rdo_sppo, run_captura_rdo_stpl],
    )

    wait_materializacao = wait_for_flow_run(
        run_materializacao,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

rdo_captura_tratamento.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
rdo_captura_tratamento.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
rdo_captura_tratamento.schedule = every_day
rdo_captura_tratamento.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
