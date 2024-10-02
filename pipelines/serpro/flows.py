# -*- coding: utf-8 -*-
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.migration.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_now_time,
    parse_timestamp_to_string,
    rename_current_flow_run_now_time,
    run_dbt_model,
    transform_raw_to_nested_structure,
    unpack_mapped_results_nout2,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
)
from pipelines.serpro.constants import constants
from pipelines.serpro.tasks import get_db_object, get_raw_serpro
from pipelines.serpro.utils import handler_setup_serpro
from pipelines.tasks import get_timestamp_range

with Flow("SMTR: SERPRO - Captura/Tratamento") as serpro_captura:
    start_date = Parameter("start_date", default=None)
    end_date = Parameter("end_date", default=None)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=serpro_captura.name + " ",
        now_time=get_now_time(),
    )

    capture_timestamps = get_timestamp_range(start_date, end_date)
    with case(start_date, None):
        current_timestamp = get_current_timestamp()

    timestamps = merge(current_timestamp, capture_timestamps)

    partitions = create_date_hour_partition.map(
        timestamps,
        partition_date_only=unmapped(True),
    )

    filenames = parse_timestamp_to_string.map(timestamps)

    local_filepaths = create_local_partition_path.map(
        dataset_id=unmapped(constants.INFRACAO_DATASET_ID.value),
        table_id=unmapped(constants.AUTUACAO_SERPRO_TABLE_ID.value),
        partitions=partitions,
        filename=filenames,
    )

    jdbc = get_db_object()

    raw_filepaths = get_raw_serpro.map(
        jdbc=unmapped(jdbc), timestamp=timestamps, local_filepath=local_filepaths
    )

    transform_raw_to_nested_structure_results = transform_raw_to_nested_structure.map(
        raw_filepath=raw_filepaths,
        filepath=local_filepaths,
        primary_key=constants.SERPRO_CAPTURE_PARAMS.value["primary_key"],
        timestamp=timestamps,
        error=unmapped(None),
    )

    errors, treated_filepaths = unpack_mapped_results_nout2(
        mapped_results=transform_raw_to_nested_structure_results
    )

    errors = upload_raw_data_to_gcs.map(
        dataset_id=unmapped(constants.INFRACAO_DATASET_ID.value),
        table_id=unmapped(constants.AUTUACAO_SERPRO_TABLE_ID.value),
        raw_filepath=raw_filepaths,
        partitions=partitions,
        error=unmapped(None),
    )

    wait_captura_true = upload_staging_data_to_gcs.map(
        dataset_id=unmapped(constants.INFRACAO_DATASET_ID.value),
        table_id=unmapped(constants.AUTUACAO_SERPRO_TABLE_ID.value),
        staging_filepath=treated_filepaths,
        partitions=partitions,
        timestamp=timestamps,
        error=errors,
    )

    wait_run_dbt_model = run_dbt_model(
        dataset_id=constants.AUTUACAO_MATERIALIZACAO_DATASET_ID.value,
        table_id=constants.AUTUACAO_MATERIALIZACAO_TABLE_ID.value,
        _vars=[{"date_range_start": start_date, "date_range_end": end_date}],
        upstream_tasks=[wait_captura_true],
    )

serpro_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
serpro_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE_FEDORA.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
serpro_captura.state_handlers = [
    handler_setup_serpro,
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
