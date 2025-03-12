# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos

DBT: 2025-02-24
"""

from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import ifelse, merge
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.migration.flows import default_capture_flow
from pipelines.migration.tasks import (
    bq_upload,
    create_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_current_flow_labels,
    get_current_flow_mode,
    get_current_timestamp,
    get_join_dict,
    get_previous_date,
    get_run_dates,
    parse_timestamp_to_string,
    rename_current_flow_run_now_time,
    run_dbt_model,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
)
from pipelines.migration.utils import set_default_parameters
from pipelines.migration.veiculo.constants import constants
from pipelines.migration.veiculo.tasks import (
    get_raw_ftp,
    get_veiculo_raw_storage,
    pre_treatment_sppo_infracao,
    pre_treatment_sppo_licenciamento,
)
from pipelines.schedules import every_day_hour_seven, every_day_hour_six_minute_fifty
from pipelines.treatment.templates.tasks import dbt_data_quality_checks, run_dbt_tests

# Flows #

# flake8: noqa: E501
with Flow(
    f"SMTR: {smtr_constants.VEICULO_DATASET_ID.value} \
{constants.SPPO_LICENCIAMENTO_TABLE_ID.value} - Captura",
) as sppo_licenciamento_captura:
    timestamp = Parameter("timestamp", default=None)
    get_from_storage = Parameter("get_from_storage", default=False)

    timestamp = get_current_timestamp(timestamp=timestamp)

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
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT
    raw_status_gcs = get_veiculo_raw_storage(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        timestamp=timestamp,
        csv_args=constants.SPPO_LICENCIAMENTO_CSV_ARGS.value,
    )

    raw_status_url = get_raw_ftp(
        ftp_path="LICENCIAMENTO/CadastrodeVeiculos",
        filetype="txt",
        csv_args=constants.SPPO_LICENCIAMENTO_CSV_ARGS.value,
        timestamp=timestamp,
    )

    ifelse(get_from_storage.is_equal(True), raw_status_gcs, raw_status_url)

    raw_status = merge(raw_status_gcs, raw_status_url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_sppo_licenciamento(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        parent_table_id=constants.SPPO_LICENCIAMENTO_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    sppo_licenciamento_captura.set_dependencies(task=partitions, upstream_tasks=[rename_flow_run])

sppo_licenciamento_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
sppo_licenciamento_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_licenciamento_captura.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]
sppo_licenciamento_captura.schedule = every_day_hour_six_minute_fifty

with Flow(
    f"SMTR: {smtr_constants.VEICULO_DATASET_ID.value} \
{constants.SPPO_INFRACAO_TABLE_ID.value} - Captura",
) as sppo_infracao_captura:
    timestamp = Parameter("timestamp", default=None)
    get_from_storage = Parameter("get_from_storage", default=False)

    timestamp = get_current_timestamp(timestamp=timestamp)

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
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT
    raw_status_gcs = get_veiculo_raw_storage(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        timestamp=timestamp,
        csv_args=constants.SPPO_INFRACAO_CSV_ARGS.value,
    )
    raw_status_url = get_raw_ftp(
        ftp_path="MULTAS/MULTAS",
        filetype="txt",
        csv_args=constants.SPPO_INFRACAO_CSV_ARGS.value,
        timestamp=timestamp,
    )
    ifelse(get_from_storage.is_equal(True), raw_status_gcs, raw_status_url)

    raw_status = merge(raw_status_gcs, raw_status_url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_sppo_infracao(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        parent_table_id=constants.SPPO_INFRACAO_TABLE_ID.value,
        timestamp=timestamp,
        error=error,
    )
    sppo_infracao_captura.set_dependencies(task=partitions, upstream_tasks=[rename_flow_run])

sppo_infracao_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
sppo_infracao_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_infracao_captura.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
sppo_infracao_captura.schedule = every_day_hour_six_minute_fifty

# flake8: noqa: E501
with Flow(
    f"SMTR: {smtr_constants.VEICULO_DATASET_ID.value} \
{constants.SPPO_VEICULO_DIA_TABLE_ID.value} - Materialização (subflow)",
) as sppo_veiculo_dia:
    # 1. SETUP #

    # Get default parameters #
    start_date = Parameter("start_date", default=get_previous_date.run(1))
    end_date = Parameter("end_date", default=get_previous_date.run(1))

    run_dates = get_run_dates(start_date, end_date)

    # Rename flow run #
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=sppo_veiculo_dia.name + ": ",
        now_time=run_dates,
    )

    # Set dbt client #
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # Get models version #
    # TODO: include version in a column in the table
    dataset_sha = fetch_dataset_sha(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
    )

    _vars = get_join_dict(dict_list=run_dates, new_dict=dataset_sha)

    # 2. TREAT #
    WAIT_DBT_RUN = run_dbt_model.map(
        dataset_id=unmapped(smtr_constants.VEICULO_DATASET_ID.value),
        table_id=unmapped(constants.SPPO_VEICULO_DIA_TABLE_ID.value),
        upstream=unmapped(True),
        exclude=unmapped("+gps_sppo"),
        _vars=_vars,
    )

    dbt_vars = get_join_dict(
        dict_list=[{"date_range_start": start_date, "date_range_end": end_date}],
        new_dict=dataset_sha,
    )[0]

    VEICULO_DATA_QUALITY_TEST = run_dbt_tests(
        dataset_id=smtr_constants.VEICULO_DATASET_ID.value,
        _vars=dbt_vars,
    ).set_upstream(WAIT_DBT_RUN)

    DATA_QUALITY_PRE = dbt_data_quality_checks(
        dbt_logs=VEICULO_DATA_QUALITY_TEST,
        checks_list=constants.VEICULO_DATA_QUALITY_CHECK_LIST.value,
        webhook_key="subsidio_data_check",
        params=dbt_vars,
    )

sppo_veiculo_dia.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
sppo_veiculo_dia.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
sppo_veiculo_dia.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]


veiculo_sppo_registro_agente_verao_captura = deepcopy(default_capture_flow)
veiculo_sppo_registro_agente_verao_captura.name = f"SMTR: \
{smtr_constants.VEICULO_DATASET_ID.value} \
{constants.SPPO_REGISTRO_AGENTE_VERAO_PARAMS.value['table_id']} - Captura"

veiculo_sppo_registro_agente_verao_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
veiculo_sppo_registro_agente_verao_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
veiculo_sppo_registro_agente_verao_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
veiculo_sppo_registro_agente_verao_captura = set_default_parameters(
    flow=veiculo_sppo_registro_agente_verao_captura,
    default_parameters=constants.SPPO_REGISTRO_AGENTE_VERAO_PARAMS.value,
)
veiculo_sppo_registro_agente_verao_captura.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]

veiculo_sppo_registro_agente_verao_captura.schedule = every_day_hour_seven
