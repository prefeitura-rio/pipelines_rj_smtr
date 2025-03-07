# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_onibus_gps_zirix

DBT 2024-08-26
"""
from prefect import Parameter, case, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_onibus_gps_zirix.constants import constants
from pipelines.migration.br_rj_riodejaneiro_onibus_gps_zirix.tasks import (
    clean_br_rj_riodejaneiro_onibus_gps_zirix,
    create_api_url_onibus_gps,
    create_api_url_onibus_realocacao,
    pre_treatment_br_rj_riodejaneiro_onibus_gps,
    pre_treatment_br_rj_riodejaneiro_onibus_realocacao,
)
from pipelines.migration.tasks import (
    bq_upload,
    create_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_current_flow_labels,
    get_current_flow_mode,
    get_current_timestamp,
    get_flow_project,
    get_materialization_date_range,
    get_now_time,
    get_raw,
    get_rounded_timestamp,
    parse_timestamp_to_string,
    query_logs,
    rename_current_flow_run_now_time,
    run_dbt_model,
    save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
)
from pipelines.schedules import every_10_minutes, every_hour_minute_six, every_minute

# Flows #

with Flow(
    "SMTR: GPS SPPO Realocação Zirix - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as realocacao_sppo_zirix:
    # SETUP
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=realocacao_sppo_zirix.name + ": ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        table_id=smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    url = create_api_url_onibus_realocacao(timestamp=timestamp)

    # EXTRACT #
    raw_status = get_raw(url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # CLEAN #
    treated_status = pre_treatment_br_rj_riodejaneiro_onibus_realocacao(
        status=raw_status, timestamp=timestamp
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        table_id=smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        parent_table_id=smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        error=error,
        timestamp=timestamp,
    )

realocacao_sppo_zirix.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
realocacao_sppo_zirix.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
realocacao_sppo_zirix.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
realocacao_sppo_zirix.schedule = every_10_minutes

with Flow(
    "SMTR: GPS SPPO Zirix Realocação - Recaptura (subflow)",
) as recaptura_realocacao_sppo_zirix:
    timestamp = Parameter("timestamp", default=None)
    recapture_window_days = Parameter("recapture_window_days", default=1)

    # SETUP #
    LABELS = get_current_flow_labels()

    # Consulta de logs para verificar erros
    errors, timestamps, previous_errors = query_logs(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        table_id=smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        datetime_filter=get_rounded_timestamp(timestamp=timestamp, interval_minutes=10),
        interval_minutes=10,
        recapture_window_days=recapture_window_days,
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=recaptura_realocacao_sppo_zirix.name + ": ",
        now_time=get_now_time(),
        wait=timestamps,
    )

    # Em caso de erros, executa a recaptura
    with case(errors, True):
        # SETUP #
        partitions = create_date_hour_partition.map(timestamps)
        filename = parse_timestamp_to_string.map(timestamps)

        filepath = create_local_partition_path.map(
            dataset_id=unmapped(constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value),
            table_id=unmapped(smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            filename=filename,
            partitions=partitions,
        )

        url = create_api_url_onibus_realocacao.map(timestamp=timestamps)

        # EXTRACT #
        raw_status = get_raw.map(url)

        raw_filepath = save_raw_local.map(status=raw_status, file_path=filepath)

        # CLEAN #
        treated_status = pre_treatment_br_rj_riodejaneiro_onibus_realocacao.map(
            status=raw_status, timestamp=timestamps
        )

        treated_filepath = save_treated_local.map(status=treated_status, file_path=filepath)

        # LOAD #
        error = bq_upload.map(
            dataset_id=unmapped(constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value),
            table_id=unmapped(smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=treated_status,
        )

        upload_logs_to_bq.map(
            dataset_id=unmapped(constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value),
            parent_table_id=unmapped(smtr_constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            error=error,
            previous_error=previous_errors,
            timestamp=timestamps,
            recapture=unmapped(True),
        )

recaptura_realocacao_sppo_zirix.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
recaptura_realocacao_sppo_zirix.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
recaptura_realocacao_sppo_zirix.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]

with Flow(
    "SMTR: GPS SPPO Zirix - Materialização (subflow)",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as materialize_sppo_zirix:
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=materialize_sppo_zirix.name + ": ", now_time=get_now_time()
    )

    # Get default parameters #
    raw_dataset_id = Parameter(
        "raw_dataset_id", default=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value
    )
    raw_table_id = Parameter("raw_table_id", default=smtr_constants.GPS_SPPO_RAW_TABLE_ID.value)
    dataset_id = Parameter("dataset_id", default=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.GPS_SPPO_ZIRIX_TREATED_TABLE_ID.value)
    rebuild = Parameter("rebuild", False)
    rematerialization = Parameter("rematerialization", default=False)
    date_range_start_param = Parameter("date_range_start", default=None)
    date_range_end_param = Parameter("date_range_end", default=None)
    materialize_delay_hours = Parameter(
        "materialize_delay_hours",
        default=constants.GPS_SPPO_MATERIALIZE_DELAY_HOURS.value,
    )

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # Set specific run parameters #
    with case(rematerialization, False):
        date_range_false = get_materialization_date_range(
            dataset_id=dataset_id,
            table_id="gps_sppo",
            raw_dataset_id=raw_dataset_id,
            raw_table_id=raw_table_id,
            table_run_datetime_column_name="timestamp_gps",
            mode=MODE,
            delay_hours=materialize_delay_hours,
            truncate_minutes=True,
        )

        RUN_CLEAN_FALSE = task(
            lambda: [None],
            checkpoint=False,
            name="assign_none_to_previous_runs",
        )()
    with case(rematerialization, True):
        date_range_true = task(
            lambda start, end: {
                "date_range_start": start,
                "date_range_end": end,
            }
        )(start=date_range_start_param, end=date_range_end_param)

        RUN_CLEAN_TRUE = clean_br_rj_riodejaneiro_onibus_gps_zirix(date_range_true)

    RUN_CLEAN = merge(RUN_CLEAN_TRUE, RUN_CLEAN_FALSE)

    date_range = merge(date_range_true, date_range_false)

    dataset_sha = fetch_dataset_sha(
        dataset_id=dataset_id,
        upstream_tasks=[RUN_CLEAN],
    )

    # Run materialization #
    with case(rebuild, True):
        RUN = run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream=True,
            exclude="+data_versao_efetiva",
            _vars=[date_range, dataset_sha],
            flags="--full-refresh",
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id=table_id,
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )
    with case(rebuild, False):
        RUN = run_dbt_model(
            dataset_id=dataset_id,
            table_id=table_id,
            exclude="+data_versao_efetiva",
            _vars=[date_range, dataset_sha],
            upstream=True,
        )
        set_last_run_timestamp(
            dataset_id=dataset_id,
            table_id="gps_sppo",
            timestamp=date_range["date_range_end"],
            wait=RUN,
            mode=MODE,
        )

materialize_sppo_zirix.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
materialize_sppo_zirix.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
materialize_sppo_zirix.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]

with Flow(
    "SMTR: GPS SPPO Zirix - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as captura_sppo_zirix:
    # SETUP #
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=captura_sppo_zirix.name + ": ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        table_id=smtr_constants.GPS_SPPO_RAW_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    url = create_api_url_onibus_gps(timestamp=timestamp)

    # EXTRACT #
    raw_status = get_raw(url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # CLEAN #
    treated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps(
        status=raw_status, timestamp=timestamp, version=2
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        table_id=smtr_constants.GPS_SPPO_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        parent_table_id=smtr_constants.GPS_SPPO_RAW_TABLE_ID.value,
        error=error,
        timestamp=timestamp,
    )

captura_sppo_zirix.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_zirix.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_sppo_zirix.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
captura_sppo_zirix.schedule = every_minute


with Flow(
    "SMTR: GPS SPPO Zirix - Tratamento",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as recaptura_zirix:
    datetime_filter = Parameter("datetime_filter", default=None)
    materialize = Parameter("materialize", default=True)

    # SETUP #
    LABELS = get_current_flow_labels()
    PROJECT = get_flow_project()

    rounded_timestamp = get_rounded_timestamp(interval_minutes=60)
    rounded_timestamp_str = parse_timestamp_to_string(
        timestamp=rounded_timestamp,
        pattern="%Y-%m-%d %H:%M:%S",
    )

    # roda o subflow de recaptura da realocação
    run_recaptura_realocacao_sppo_zirix = create_flow_run(
        flow_name=recaptura_realocacao_sppo_zirix.name,
        project_name=PROJECT,
        labels=LABELS,
        run_name=recaptura_realocacao_sppo_zirix.name,
        parameters={"timestamp": rounded_timestamp_str},
    )

    wait_recaptura_realocacao_sppo_zirix = wait_for_flow_run(
        run_recaptura_realocacao_sppo_zirix,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    errors, timestamps, previous_errors = query_logs(
        dataset_id=constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value,
        table_id=smtr_constants.GPS_SPPO_RAW_TABLE_ID.value,
        datetime_filter=datetime_filter,
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=recaptura_zirix.name + ": ", now_time=get_now_time(), wait=timestamps
    )
    with case(errors, False):
        with case(materialize, True):
            materialize_no_error = create_flow_run(
                flow_name=materialize_sppo_zirix.name,
                project_name=PROJECT,
                labels=LABELS,
                run_name=materialize_sppo_zirix.name,
            )
            wait_materialize_no_error = wait_for_flow_run(
                materialize_no_error,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
    with case(errors, True):
        # SETUP #
        partitions = create_date_hour_partition.map(timestamps)
        filename = parse_timestamp_to_string.map(timestamps)

        filepath = create_local_partition_path.map(
            dataset_id=unmapped(constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value),
            table_id=unmapped(smtr_constants.GPS_SPPO_RAW_TABLE_ID.value),
            filename=filename,
            partitions=partitions,
        )

        url = create_api_url_onibus_gps.map(timestamp=timestamps)

        # EXTRACT #
        raw_status = get_raw.map(url)

        raw_filepath = save_raw_local.map(status=raw_status, file_path=filepath)

        # # CLEAN #
        trated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps.map(
            status=raw_status,
            timestamp=timestamps,
            version=unmapped(2),
        )

        treated_filepath = save_treated_local.map(status=trated_status, file_path=filepath)

        # # LOAD #
        error = bq_upload.map(
            dataset_id=unmapped(constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value),
            table_id=unmapped(smtr_constants.GPS_SPPO_RAW_TABLE_ID.value),
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=trated_status,
        )

        UPLOAD_LOGS = upload_logs_to_bq.map(
            dataset_id=unmapped(constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value),
            parent_table_id=unmapped(smtr_constants.GPS_SPPO_RAW_TABLE_ID.value),
            error=error,
            previous_error=previous_errors,
            timestamp=timestamps,
            recapture=unmapped(True),
        )
        with case(materialize, True):
            run_materialize = create_flow_run(
                flow_name=materialize_sppo_zirix.name,
                project_name=PROJECT,
                labels=LABELS,
                run_name=materialize_sppo_zirix.name,
            )
            wait_materialize = wait_for_flow_run(
                run_materialize,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
    recaptura_zirix.set_dependencies(
        task=run_materialize,
        upstream_tasks=[UPLOAD_LOGS],
    )

recaptura_zirix.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
recaptura_zirix.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
recaptura_zirix.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
recaptura_zirix.schedule = every_hour_minute_six
