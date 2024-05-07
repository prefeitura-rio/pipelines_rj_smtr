# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos
"""

from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
# from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode

# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.constants import constants as emd_constants
from pipelines.schedules import every_day_hour_seven

# from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.utils.backup.flows import default_capture_flow
from pipelines.utils.backup.tasks import (
    bq_upload,
    create_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_current_flow_labels,
    get_current_flow_mode,
    get_current_timestamp,
    get_join_dict,
    get_previous_date,
    get_raw,
    get_run_dates,
    parse_timestamp_to_string,
    rename_current_flow_run_now_time,
    run_dbt_model,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
)
from pipelines.utils.backup.utils import set_default_parameters
from pipelines.br_rj_riodejaneiro_rdo.tasks import (
    check_files_for_download, 
    download_and_save_local_from_ftp, 
    update_redis_ftp_files
)
from pipelines.veiculo.tasks import (
    get_ftp_filepaths,
    pre_treatment_sppo_infracao,
    pre_treatment_sppo_licenciamento,
)

# EMD Imports #


# SMTR Imports #


# Flows #

with Flow('SMTR - Captura STU FTP') as captura_stu_ftp:

    search_dir = Parameter('search_dir', default='multas')
    dataset_id = Parameter('dataset_id', default=constants.VEICULO_DATASET_ID.value)
    table_id = Parameter('table_id', default=constants.SPPO_INFRACAO_TABLE_ID.value)
    #     rename_run = rename_current_flow_run_now_time(
    #     prefix=f"{captura_sppo_rho.name} FTP - {transport_mode.run()}-{report_type.run()} ",
    #     now_time=get_current_timestamp(),
    #     wait=None,
    # )
    MODE = get_current_flow_mode()
    # EXTRACT
    files = get_ftp_filepaths(
        search_dir=search_dir
    )
    download_files = check_files_for_download(
        files=files, dataset_id=dataset_id, table_id=table_id
    )
    updated_files_info = download_and_save_local_from_ftp.map(
        file_info=download_files, 
        dataset_id=dataset_id, 
        table_id=table_id
    )
    # TRANSFORM
    treated_paths, raw_paths, partitions, status = pre_treatment_sppo_infracao(files = updated_files_info)

    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        filepath=treated_paths,
        raw_filepath=raw_paths,
        partitions=partitions,
        status=status,
    )
    set_redis = update_redis_ftp_files(
        download_files=download_files,
        dataset_id=dataset_id,
        table_id=table_id, 
        errors=errors,
        mode=MODE
        )

# flake8: noqa: E501
with Flow(
    f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_LICENCIAMENTO_TABLE_ID.value} - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_licenciamento_captura:
    timestamp = get_current_timestamp()

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=f"{sppo_licenciamento_captura.name} - ", now_time=timestamp
    )

    # SETUP #
    partitions = create_date_hour_partition(timestamp, partition_date_only=True)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT
    raw_status = get_raw(
        url=constants.SPPO_LICENCIAMENTO_URL.value,
        filetype="txt",
        csv_args=constants.SPPO_LICENCIAMENTO_CSV_ARGS.value,
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_sppo_licenciamento(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        parent_table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    sppo_licenciamento_captura.set_dependencies(task=partitions, upstream_tasks=[rename_flow_run])

sppo_licenciamento_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_licenciamento_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_licenciamento_captura.schedule = every_day_hour_seven
sppo_licenciamento_captura.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]

with Flow(
    f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_INFRACAO_TABLE_ID.value} - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_infracao_captura:
    timestamp = get_current_timestamp()

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=f"{sppo_infracao_captura.name} - ", now_time=timestamp
    )

    # SETUP #
    partitions = create_date_hour_partition(timestamp, partition_date_only=True)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT
    raw_status = get_raw(
        url=constants.SPPO_INFRACAO_URL.value,
        filetype="txt",
        csv_args=constants.SPPO_INFRACAO_CSV_ARGS.value,
    )

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_sppo_infracao(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.VEICULO_DATASET_ID.value,
        parent_table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    sppo_infracao_captura.set_dependencies(task=partitions, upstream_tasks=[rename_flow_run])

sppo_infracao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_infracao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_infracao_captura.schedule = every_day_hour_seven
sppo_infracao_captura.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]

# flake8: noqa: E501
with Flow(
    f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_VEICULO_DIA_TABLE_ID.value} - Materialização (subflow)",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as sppo_veiculo_dia:
    # 1. SETUP #

    # Get default parameters #
    start_date = Parameter("start_date", default=get_previous_date.run(1))
    end_date = Parameter("end_date", default=get_previous_date.run(1))
    stu_data_versao = Parameter("stu_data_versao", default="")

    run_dates = get_run_dates(start_date, end_date)

    # Rename flow run #
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=sppo_veiculo_dia.name + ": ",
        now_time=run_dates,
    )

    # Set dbt client #
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Get models version #
    # TODO: include version in a column in the table
    dataset_sha = fetch_dataset_sha(
        dataset_id=constants.VEICULO_DATASET_ID.value,
    )

    dict_list = get_join_dict(dict_list=run_dates, new_dict=dataset_sha)
    _vars = get_join_dict(dict_list=dict_list, new_dict={"stu_data_versao": stu_data_versao})

    # 2. TREAT #
    run_dbt_model.map(
        # dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(constants.VEICULO_DATASET_ID.value),
        table_id=unmapped(constants.SPPO_VEICULO_DIA_TABLE_ID.value),
        upstream=unmapped(True),
        exclude=unmapped("+gps_sppo"),
        _vars=_vars,
    )

sppo_veiculo_dia.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
sppo_veiculo_dia.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_veiculo_dia.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]

veiculo_sppo_registro_agente_verao_captura = deepcopy(default_capture_flow)
veiculo_sppo_registro_agente_verao_captura.name = f"SMTR: {constants.VEICULO_DATASET_ID.value} {constants.SPPO_REGISTRO_AGENTE_VERAO_PARAMS.value['table_id']} - Captura"
veiculo_sppo_registro_agente_verao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
veiculo_sppo_registro_agente_verao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
veiculo_sppo_registro_agente_verao_captura.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]

veiculo_sppo_registro_agente_verao_captura = set_default_parameters(
    flow=veiculo_sppo_registro_agente_verao_captura,
    default_parameters=constants.SPPO_REGISTRO_AGENTE_VERAO_PARAMS.value,
)
veiculo_sppo_registro_agente_verao_captura.schedule = every_day_hour_seven
