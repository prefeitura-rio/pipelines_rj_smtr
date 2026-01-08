# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_onibus_gps

DBT 2025-11-17
"""

from copy import deepcopy

from prefect import Parameter, case, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow

# SMTR Imports #
# from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants
from pipelines.constants import constants as emd_constants
from pipelines.migration.br_rj_riodejaneiro_onibus_gps.constants import (
    constants as gps_constants,
)
from pipelines.migration.br_rj_riodejaneiro_onibus_gps.tasks import (
    clean_br_rj_riodejaneiro_onibus_gps,
    create_api_url_onibus_gps,
    create_api_url_onibus_realocacao,
    pre_treatment_br_rj_riodejaneiro_onibus_gps,
    pre_treatment_br_rj_riodejaneiro_onibus_realocacao,
)
from pipelines.migration.tasks import (  # get_local_dbt_client,
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
    save_raw_local,
    save_treated_local,
    set_last_run_timestamp,
    upload_logs_to_bq,
)
from pipelines.migration.utils import set_default_parameters
from pipelines.schedules import (
    every_10_minutes,
    every_15_minutes,
    every_hour_minute_six,
    every_minute,
)
from pipelines.treatment.templates.tasks import (
    check_dbt_test_run,
    dbt_data_quality_checks,
    run_dbt,
)

# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client


# Flows #

with Flow(
    "SMTR: GPS SPPO Realocação - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as realocacao_sppo:
    # SETUP #

    # Get default parameters #
    raw_dataset_id = Parameter("raw_dataset_id", default=constants.GPS_SPPO_RAW_DATASET_ID.value)
    raw_table_id = Parameter(
        "raw_table_id", default=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value
    )
    dataset_id = Parameter("dataset_id", default=constants.GPS_SPPO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.GPS_SPPO_REALOCACAO_TREATED_TABLE_ID.value)
    rebuild = Parameter("rebuild", False)

    # SETUP
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=realocacao_sppo.name + ": ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
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
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        parent_table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        error=error,
        timestamp=timestamp,
    )

realocacao_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
realocacao_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
realocacao_sppo.schedule = every_10_minutes
realocacao_sppo.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]


with Flow(
    "SMTR: GPS SPPO - Materialização (subflow)",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as materialize_sppo:
    # Rename flow run
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=materialize_sppo.name + ": ", now_time=get_now_time()
    )

    # Get default parameters #
    raw_dataset_id = Parameter("raw_dataset_id", default=constants.GPS_SPPO_RAW_DATASET_ID.value)
    raw_table_id = Parameter("raw_table_id", default=constants.GPS_SPPO_RAW_TABLE_ID.value)
    dataset_id = Parameter("dataset_id", default=constants.GPS_SPPO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.GPS_SPPO_TREATED_TABLE_ID.value)
    rebuild = Parameter("rebuild", False)
    rematerialization = Parameter("rematerialization", default=False)
    date_range_start_param = Parameter("date_range_start", default=None)
    date_range_end_param = Parameter("date_range_end", default=None)
    _15_minutos = Parameter("15_minutos", default=False)
    materialize_delay_hours = Parameter(
        "materialize_delay_hours",
        default=constants.GPS_SPPO_MATERIALIZE_DELAY_HOURS.value,
    )
    truncate_minutes = Parameter("truncate_minutes", default=True)
    test_only = Parameter("test_only", default=False)
    run_time_test = Parameter("run_time_test", default="01:00:00")

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # Set dbt client #
    # dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Set specific run parameters #
    with case(test_only, False):
        with case(rematerialization, False):
            date_range_false = get_materialization_date_range(
                dataset_id=dataset_id,
                table_id=table_id,
                raw_dataset_id=raw_dataset_id,
                raw_table_id=raw_table_id,
                table_run_datetime_column_name="timestamp_gps",
                mode=MODE,
                delay_hours=materialize_delay_hours,
                truncate_minutes=truncate_minutes,
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

            RUN_CLEAN_TRUE = clean_br_rj_riodejaneiro_onibus_gps(date_range_true)

        RUN_CLEAN = merge(RUN_CLEAN_TRUE, RUN_CLEAN_FALSE)

        date_range = merge(date_range_true, date_range_false)

        dataset_sha = fetch_dataset_sha(
            dataset_id=dataset_id,
            upstream_tasks=[RUN_CLEAN],
        )

        # Run materialization #
        with case(rebuild, True):
            RUN_TRUE = run_dbt(
                resource="model",
                # dbt_client=dbt_client,
                dataset_id=dataset_id,
                table_id=table_id,
                upstream=True,
                exclude="+data_versao_efetiva",
                _vars=[date_range, dataset_sha, {"15_minutos": _15_minutos}],
                flags="--full-refresh",
            )

        with case(rebuild, False):
            RUN_FALSE = run_dbt(
                resource="model",
                # dbt_client=dbt_client,
                dataset_id=dataset_id,
                table_id=table_id,
                exclude="+data_versao_efetiva",
                _vars=[date_range, dataset_sha, {"15_minutos": _15_minutos}],
                upstream=True,
            )

            date_range_start = date_range["date_range_start"]
            date_range_end = date_range["date_range_end"]

            RUN_TEST, datetime_start, datetime_end = check_dbt_test_run(
                date_range_start, date_range_end, run_time_test, upstream_tasks=[RUN_FALSE]
            )

            _vars = {"date_range_start": datetime_start, "date_range_end": datetime_end}

            with case(RUN_TEST, True):
                gps_sppo_data_quality = run_dbt(
                    resource="test",
                    dataset_id=dataset_id,
                    table_id=table_id,
                    _vars=_vars,
                )
                GPS_SPPO_DATA_QUALITY_RESULTS = dbt_data_quality_checks(
                    gps_sppo_data_quality,
                    gps_constants.GPS_DATA_CHECKS_LIST.value,
                    _vars,
                )

        RUN = merge(RUN_TRUE, RUN_FALSE)

        with case(rematerialization, False):
            SET_FALSE = set_last_run_timestamp(
                dataset_id=dataset_id,
                table_id=table_id,
                timestamp=date_range["date_range_end"],
                wait=RUN,
                mode=MODE,
            )

        with case(rematerialization, True):
            SET_TRUE = task(
                lambda: [None],
                checkpoint=False,
                name="assign_none_to_previous_runs",
            )()

        SET = merge(SET_TRUE, SET_FALSE)

        materialize_sppo.set_reference_tasks([RUN, RUN_CLEAN, SET])
    with case(test_only, True):

        _vars = {"date_range_start": date_range_start_param, "date_range_end": date_range_end_param}

        gps_sppo_data_quality = run_dbt(
            resource="test",
            dataset_id=dataset_id,
            table_id=table_id,
            _vars=_vars,
        )
        GPS_SPPO_DATA_QUALITY_RESULTS = dbt_data_quality_checks(
            gps_sppo_data_quality,
            gps_constants.GPS_DATA_CHECKS_LIST.value,
            _vars,
        )

materialize_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
materialize_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
materialize_sppo.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
]

with Flow(
    "SMTR: GPS SPPO - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as captura_sppo_v2:
    version = Parameter("version", default=2)

    # SETUP #
    timestamp = get_current_timestamp()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=captura_sppo_v2.name + ": ", now_time=timestamp
    )

    partitions = create_date_hour_partition(timestamp)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        filename=filename,
        partitions=partitions,
    )

    url = create_api_url_onibus_gps(version=version, timestamp=timestamp)

    # EXTRACT #
    raw_status = get_raw(url)

    raw_filepath = save_raw_local(status=raw_status, file_path=filepath)

    # CLEAN #
    treated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps(
        status=raw_status, timestamp=timestamp, version=version
    )

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD #
    error = bq_upload(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        filepath=treated_filepath,
        raw_filepath=raw_filepath,
        partitions=partitions,
        status=treated_status,
    )

    upload_logs_to_bq(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        parent_table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        error=error,
        timestamp=timestamp,
    )

captura_sppo_v2.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_sppo_v2.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_sppo_v2.schedule = every_minute
captura_sppo_v2.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

with Flow(
    "SMTR: GPS SPPO Realocação - Recaptura (subflow)",
) as recaptura_realocacao_sppo:
    timestamp = Parameter("timestamp", default=None)
    recapture_window_days = Parameter("recapture_window_days", default=1)

    # SETUP #
    LABELS = get_current_flow_labels()

    # Consulta de logs para verificar erros
    errors, timestamps, previous_errors = query_logs(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value,
        datetime_filter=get_rounded_timestamp(timestamp=timestamp, interval_minutes=10),
        interval_minutes=10,
        recapture_window_days=recapture_window_days,
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=recaptura_realocacao_sppo.name + ": ",
        now_time=get_now_time(),
        wait=timestamps,
    )

    # Em caso de erros, executa a recaptura
    with case(errors, True):
        # SETUP #
        partitions = create_date_hour_partition.map(timestamps)
        filename = parse_timestamp_to_string.map(timestamps)

        filepath = create_local_partition_path.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
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
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=treated_status,
        )

        upload_logs_to_bq.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            parent_table_id=unmapped(constants.GPS_SPPO_REALOCACAO_RAW_TABLE_ID.value),
            error=error,
            previous_error=previous_errors,
            timestamp=timestamps,
            recapture=unmapped(True),
        )

recaptura_realocacao_sppo.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
recaptura_realocacao_sppo.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
recaptura_realocacao_sppo.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]

with Flow("SMTR: GPS SPPO - Tratamento") as recaptura:
    version = Parameter("version", default=2)
    datetime_filter_gps = Parameter("datetime_filter_gps", default=None)
    materialize = Parameter("materialize", default=True)
    # SETUP #
    LABELS = get_current_flow_labels()
    PROJECT = get_flow_project()
    rounded_timestamp = get_rounded_timestamp(interval_minutes=60)
    rounded_timestamp_str = parse_timestamp_to_string(
        timestamp=rounded_timestamp, pattern="%Y-%m-%d %H:%M:%S"
    )

    # roda o subflow de recaptura da realocação
    run_recaptura_realocacao_sppo = create_flow_run(
        flow_name=recaptura_realocacao_sppo.name,
        project_name=PROJECT,
        labels=LABELS,
        run_name=recaptura_realocacao_sppo.name,
        parameters={"timestamp": rounded_timestamp_str},
    )

    wait_recaptura_realocacao_sppo = wait_for_flow_run(
        run_recaptura_realocacao_sppo,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )

    errors, timestamps, previous_errors = query_logs(
        dataset_id=constants.GPS_SPPO_RAW_DATASET_ID.value,
        table_id=constants.GPS_SPPO_RAW_TABLE_ID.value,
        datetime_filter=datetime_filter_gps,
        upstream_tasks=[wait_recaptura_realocacao_sppo],
    )

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=recaptura.name + ": ", now_time=get_now_time(), wait=timestamps
    )
    with case(errors, False):
        with case(materialize, True):
            materialize_no_error = create_flow_run(
                flow_name=materialize_sppo.name,
                project_name=PROJECT,
                labels=LABELS,
                run_name=materialize_sppo.name,
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
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_RAW_TABLE_ID.value),
            filename=filename,
            partitions=partitions,
        )

        url = create_api_url_onibus_gps.map(version=unmapped(version), timestamp=timestamps)

        # EXTRACT #
        raw_status = get_raw.map(url)

        raw_filepath = save_raw_local.map(status=raw_status, file_path=filepath)

        # # CLEAN #
        trated_status = pre_treatment_br_rj_riodejaneiro_onibus_gps.map(
            status=raw_status,
            timestamp=timestamps,
            version=unmapped(version),
        )

        treated_filepath = save_treated_local.map(status=trated_status, file_path=filepath)

        # # LOAD #
        error = bq_upload.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            table_id=unmapped(constants.GPS_SPPO_RAW_TABLE_ID.value),
            filepath=treated_filepath,
            raw_filepath=raw_filepath,
            partitions=partitions,
            status=trated_status,
        )

        UPLOAD_LOGS = upload_logs_to_bq.map(
            dataset_id=unmapped(constants.GPS_SPPO_RAW_DATASET_ID.value),
            parent_table_id=unmapped(constants.GPS_SPPO_RAW_TABLE_ID.value),
            error=error,
            previous_error=previous_errors,
            timestamp=timestamps,
            recapture=unmapped(True),
        )
        with case(materialize, True):
            run_materialize = create_flow_run(
                flow_name=materialize_sppo.name,
                project_name=PROJECT,
                labels=LABELS,
                run_name=materialize_sppo.name,
            )
            wait_materialize = wait_for_flow_run(
                run_materialize,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
    recaptura.set_dependencies(
        task=run_materialize,
        upstream_tasks=[UPLOAD_LOGS],
    )

recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
recaptura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
recaptura.schedule = every_hour_minute_six
recaptura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
]


materialize_gps_15_min = deepcopy(materialize_sppo)
materialize_gps_15_min = set_default_parameters(
    flow=materialize_gps_15_min,
    default_parameters={
        "raw_table_id": "sppo_registros",
        "table_id": constants.GPS_SPPO_15_MIN_TREATED_TABLE_ID.value,
        "materialize_delay_hours": 0,
        "truncate_minutes": False,
        "15_minutos": True,
    },
)
materialize_gps_15_min.name = "SMTR: GPS SPPO 15 Minutos - Materialização (subflow)"

with Flow("SMTR: GPS SPPO 15 Minutos - Tratamento") as recaptura_15min:
    version = Parameter("version", default=2)
    datetime_filter_gps = Parameter("datetime_filter_gps", default=None)
    rebuild = Parameter("rebuild", default=False)
    # SETUP #
    LABELS = get_current_flow_labels()
    PROJECT = get_flow_project()

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=recaptura.name + ": ", now_time=get_now_time()
    )

    materialize_no_error = create_flow_run(
        flow_name=materialize_gps_15_min.name,
        project_name=PROJECT,
        labels=LABELS,
        run_name=materialize_gps_15_min.name,
        parameters={
            "table_id": constants.GPS_SPPO_15_MIN_TREATED_TABLE_ID.value,
            "rebuild": rebuild,
            "materialize_delay_hours": 0,
            "truncate_minutes": False,
            "15_minutos": True,
        },
    )

    wait_materialize_no_error = wait_for_flow_run(
        materialize_no_error,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )


recaptura_15min.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
recaptura_15min.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
recaptura_15min.schedule = every_15_minutes
recaptura_15min.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
]
