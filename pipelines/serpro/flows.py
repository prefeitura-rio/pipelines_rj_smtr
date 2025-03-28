# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.migration.tasks import (  # run_dbt_model,
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_now_time,
    get_previous_date,
    parse_timestamp_to_string,
    rename_current_flow_run_now_time,
    transform_raw_to_nested_structure_chunked,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
)
from pipelines.serpro.constants import constants
from pipelines.serpro.tasks import get_db_object, get_raw_serpro
from pipelines.serpro.utils import handler_setup_serpro

with Flow("SMTR: SERPRO - Filtro") as serpro_filtro:
    start_date = Parameter("start_date", default=get_previous_date.run(1))
    end_date = Parameter("end_date", default=get_previous_date.run(1))

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=serpro_filtro.name + " ",
        now_time=get_now_time(),
    )

    timestamp = get_current_timestamp()

    partitions = create_date_hour_partition(
        timestamp,
        partition_date_only=unmapped(True),
    )

    filenames = parse_timestamp_to_string(timestamp)

    local_filepaths = create_local_partition_path(
        dataset_id=constants.INFRACAO_DATASET_ID.value,
        table_id=constants.AUTUACAO_SERPRO_TABLE_ID.value,
        partitions=partitions,
        filename=filenames,
    )

    jdbc = get_db_object()

    raw_filepaths = get_raw_serpro(
        jdbc=jdbc, start_date=start_date, end_date=end_date, local_filepath=local_filepaths
    )

    errors, treated_filepaths = transform_raw_to_nested_structure_chunked(
        raw_filepath=raw_filepaths,
        filepath=local_filepaths,
        primary_key=constants.SERPRO_CAPTURE_PARAMS.value["primary_key"],
        timestamp=timestamp,
        reader_args=constants.SERPRO_CAPTURE_PARAMS.value["pre_treatment_reader_args"],
        error=None,
        chunksize=50000,
    )

    errors = upload_raw_data_to_gcs(
        dataset_id=constants.INFRACAO_DATASET_ID.value,
        table_id=constants.AUTUACAO_SERPRO_TABLE_ID.value,
        raw_filepath=raw_filepaths,
        partitions=partitions,
        error=None,
        bucket_name=constants.INFRACAO_PRIVATE_BUCKET.value,
    )

    wait_captura_true = upload_staging_data_to_gcs(
        dataset_id=constants.INFRACAO_DATASET_ID.value,
        table_id=constants.AUTUACAO_SERPRO_TABLE_ID.value,
        staging_filepath=treated_filepaths,
        partitions=partitions,
        timestamp=timestamp,
        error=errors,
        bucket_name=constants.INFRACAO_PRIVATE_BUCKET.value,
    )

    # wait_run_dbt_model = run_dbt_model(
    #     dataset_id=constants.AUTUACAO_MATERIALIZACAO_DATASET_ID.value,
    #     table_id=constants.AUTUACAO_MATERIALIZACAO_TABLE_ID.value,
    #     _vars=[{"date_range_start": start_date, "date_range_end": end_date}],
    #     upstream=True,
    #     upstream_tasks=[wait_captura_true],
    # )

serpro_filtro.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
serpro_filtro.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE_FEDORA.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
serpro_filtro.state_handlers = [
    handler_setup_serpro,
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
