# -*- coding: utf-8 -*-
"""
Flows for gtfs

DBT 2024-07-02
"""

from prefect import Parameter, case, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow

# from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.constants import constants

# SMTR Imports #
from pipelines.migration.br_rj_riodejaneiro_gtfs.tasks import (
    get_last_capture_os,
    get_os_info,
    get_raw_drive_files,
    update_last_captured_os,
)
from pipelines.migration.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    fetch_dataset_sha,
    get_current_flow_mode,
    get_current_timestamp,
    get_join_dict,
    rename_current_flow_run_now_time,
    run_dbt_model,
    transform_raw_to_nested_structure,
    unpack_mapped_results_nout2,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
)
from pipelines.schedules import every_5_minutes
from pipelines.tasks import get_scheduled_timestamp, parse_timestamp_to_string

# from pipelines.capture.templates.flows import create_default_capture_flow


# Imports #


# EMD Imports #


# from pipelines.rj_smtr.flows import default_capture_flow, default_materialization_flow

# SETUP dos Flows

# gtfs_captura = create_default_capture_flow(
#     flow_name="SMTR: GTFS - Captura (subflow)",
#     agent_label=constants.RJ_SMTR_AGENT_LABEL.value,
#     overwrite_flow_params=constants.GTFS_GENERAL_CAPTURE_PARAMS.value,
# )

# Captura Antiga
# gtfs_captura = deepcopy(default_capture_flow)
# gtfs_captura.name = "SMTR: GTFS - Captura (subflow)"
# gtfs_captura.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# gtfs_captura.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value,
#     labels=[constants.RJ_SMTR_AGENT_LABEL.value],
# )
# gtfs_captura.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
# gtfs_captura = set_default_parameters(
#     flow=gtfs_captura,
#     default_parameters=constants.GTFS_GENERAL_CAPTURE_PARAMS.value,
# )
# Captura Antiga

# gtfs_materializacao = create_default_materialization_flow(
#     flow_name="SMTR: GTFS - Materialização (subflow)",
#     agent_label=constants.RJ_SMTR_AGENT_LABEL.value,
#     overwrite_flow_param_values=constants.GTFS_MATERIALIZACAO_PARAMS.value,
# )

# gtfs_materializacao = deepcopy(default_materialization_flow)
# gtfs_materializacao.name = "SMTR: GTFS - Materialização (subflow)"
# gtfs_materializacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# gtfs_materializacao.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value,
#     labels=[constants.RJ_SMTR_AGENT_LABEL.value],
# )
# gtfs_materializacao.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
# gtfs_materializacao = set_default_parameters(
#     flow=gtfs_materializacao,
#     default_parameters=constants.GTFS_MATERIALIZACAO_PARAMS.value,
# )

with Flow("SMTR: GTFS - Captura/Tratamento") as gtfs_captura_nova:
    capture = Parameter("capture", default=True)
    materialize = Parameter("materialize", default=True)
    regular_sheet_index = Parameter("regular_sheet_index", default=None)
    data_versao_gtfs_param = Parameter("data_versao_gtfs", default=None)

    mode = get_current_flow_mode()

    last_captured_os_none = None
    with case(data_versao_gtfs_param, None):
        last_captured_os_redis = get_last_capture_os(
            mode=mode, dataset_id=constants.GTFS_DATASET_ID.value
        )

    last_captured_os = merge(last_captured_os_none, last_captured_os_redis)

    with case(capture, True):

        timestamp = get_scheduled_timestamp()

        flag_new_os, os_control, data_index, data_versao_gtfs_task = get_os_info(
            last_captured_os=last_captured_os, data_versao_gtfs=data_versao_gtfs_param
        )

        with case(flag_new_os, True):
            rename_current_flow_run_now_time(
                prefix=gtfs_captura_nova.name + ' ["' + data_versao_gtfs_task + '"] ',
                now_time=timestamp,
            )

            data_versao_gtfs_task = get_current_timestamp(data_versao_gtfs_task)

            partition = create_date_hour_partition(
                timestamp=data_versao_gtfs_task,
                partition_date_name="data_versao",
                partition_date_only=True,
            )

            filename = parse_timestamp_to_string(data_versao_gtfs_task)

            table_ids = task(lambda: list(constants.GTFS_TABLE_CAPTURE_PARAMS.value.keys()))()

            local_filepaths = create_local_partition_path.map(
                dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
                table_id=table_ids,
                partitions=unmapped(partition),
                filename=unmapped(filename),
            )

            raw_filepaths, primary_keys = get_raw_drive_files(
                os_control=os_control,
                local_filepath=local_filepaths,
                regular_sheet_index=regular_sheet_index,
            )

            transform_raw_to_nested_structure_results = transform_raw_to_nested_structure.map(
                raw_filepath=raw_filepaths,
                filepath=local_filepaths,
                primary_key=primary_keys,
                timestamp=unmapped(data_versao_gtfs_task),
                error=unmapped(None),
            )

            errors, treated_filepaths = unpack_mapped_results_nout2(
                mapped_results=transform_raw_to_nested_structure_results
            )

            errors = upload_raw_data_to_gcs.map(
                dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
                table_id=table_ids,
                raw_filepath=raw_filepaths,
                partitions=unmapped(partition),
                error=unmapped(None),
            )

            wait_captura_true = upload_staging_data_to_gcs.map(
                dataset_id=unmapped(constants.GTFS_DATASET_ID.value),
                table_id=table_ids,
                staging_filepath=treated_filepaths,
                partitions=unmapped(partition),
                timestamp=unmapped(data_versao_gtfs_task),
                error=errors,
            )
    with case(capture, False):
        wait_captura_false = task()

    data_versao_gtfs_merge = merge(data_versao_gtfs_task, data_versao_gtfs_param)
    wait_captura = merge(wait_captura_true, wait_captura_false)

    with case(materialize, True):
        with case(data_versao_gtfs_merge, str):
            string_data_versao_gtfs = parse_timestamp_to_string(
                timestamp=data_versao_gtfs_merge, pattern="%Y-%m-%d"
            )
        data_versao_gtfs = merge(string_data_versao_gtfs, data_versao_gtfs_merge)
        version = fetch_dataset_sha(dataset_id=constants.GTFS_MATERIALIZACAO_DATASET_ID.value)
        dbt_vars = get_join_dict([{"data_versao_gtfs": data_versao_gtfs}], version)[0]

        wait_run_dbt_model = run_dbt_model(
            dataset_id=constants.GTFS_MATERIALIZACAO_DATASET_ID.value,
            _vars=dbt_vars,
        ).set_upstream(task=wait_captura)

        wait_materialize_true = update_last_captured_os(
            dataset_id=constants.GTFS_DATASET_ID.value,
            data_index=data_index,
            mode=mode,
        ).set_upstream(task=wait_run_dbt_model)

    with case(materialize, False):
        wait_materialize_false = task()

    wait_materialize = merge(wait_materialize_true, wait_materialize_false)

    with case(flag_new_os, False):
        rename_current_flow_run_now_time(
            prefix=gtfs_captura_nova.name + " [SKIPPED] ", now_time=timestamp
        ).set_upstream(task=wait_materialize)


gtfs_captura_nova.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
gtfs_captura_nova.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMTR_AGENT_LABEL.value],
)
gtfs_captura_nova.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
    handler_skip_if_running,
]
gtfs_captura_nova.schedule = every_5_minutes


# with Flow(
#     "SMTR: GTFS - Captura/Tratamento",
#     # code_owners=["rodrigo", "carolinagomes"],
# ) as gtfs_captura_tratamento:
#     # SETUP
#     data_versao_gtfs = Parameter("data_versao_gtfs", default=None)
#     capture = Parameter("capture", default=True)
#     materialize = Parameter("materialize", default=True)

#     timestamp = get_current_timestamp()

#     rename_flow_run = rename_current_flow_run_now_time(
#         prefix=gtfs_captura_tratamento.name + " " + data_versao_gtfs + " ",
#         now_time=timestamp,
#     )

#     LABELS = get_current_flow_labels()
#     PROJECT = get_flow_project()

#     with case(capture, True):

#         # Captura nova
#         run_captura = create_flow_run.map(
#             flow_name=unmapped(gtfs_captura_nova.name),
#             project_name=unmapped(PROJECT),
#             parameters=None,
#             labels=unmapped(LABELS),
#             scheduled_start_time=get_scheduled_start_times(
#                 timestamp=timestamp,
#                 parameters=None,
#                 intervals={"agency": timedelta(minutes=5)},
#             ),
#         )

#         wait_captura_true = wait_for_flow_run.map(
#             run_captura,
#             stream_states=unmapped(True),
#             stream_logs=unmapped(True),
#             raise_final_state=unmapped(True),
#         )

#         # Captura antiga

#         # gtfs_capture_parameters = [
#         #     {"timestamp": data_versao_gtfs, **d} for d in
# constants.GTFS_TABLE_CAPTURE_PARAMS.value
#         # ]

#         # run_captura = create_flow_run.map(
#         #     flow_name=unmapped(gtfs_captura.name),
#         #     project_name=unmapped(PROJECT),
#         #     parameters=gtfs_capture_parameters,
#         #     labels=unmapped(LABELS),
#         #     scheduled_start_time=get_scheduled_start_times(
#         #         timestamp=timestamp,
#         #         parameters=gtfs_capture_parameters,
#         #         intervals={"agency": timedelta(minutes=11)},
#         #     ),
#         # )

#         # wait_captura_true = wait_for_flow_run.map(
#         #     run_captura,
#         #     stream_states=unmapped(True),
#         #     stream_logs=unmapped(True),
#         #     raise_final_state=unmapped(True),
#         # )

#     with case(capture, False):
#         wait_captura_false = task(
#             lambda: [None], checkpoint=False, name="assign_none_to_previous_runs"
#         )()

#     wait_captura = merge(wait_captura_true, wait_captura_false)

#     with case(materialize, True):
#         gtfs_materializacao_parameters = {
#             "dbt_vars": {
#                 "data_versao_gtfs": data_versao_gtfs,
#                 "version": {},
#             },
#         }
#         gtfs_materializacao_parameters_new = {
#             "dataset_id": "gtfs",
#             "dbt_vars": {
#                 "data_versao_gtfs": data_versao_gtfs,
#                 "version": {},
#             },
#         }

#         run_materializacao = create_flow_run(
#             flow_name=gtfs_materializacao.name,
#             project_name=PROJECT,
#             parameters=gtfs_materializacao_parameters,
#             labels=LABELS,
#             upstream_tasks=[wait_captura],
#         )

#         run_materializacao_new_dataset_id = create_flow_run(
#             flow_name=gtfs_materializacao.name,
#             project_name=PROJECT,
#             parameters=gtfs_materializacao_parameters_new,
#             labels=LABELS,
#             upstream_tasks=[wait_captura],
#         )

#         wait_materializacao = wait_for_flow_run(
#             run_materializacao,
#             stream_states=True,
#             stream_logs=True,
#             raise_final_state=True,
#         )

#         wait_materializacao_new_dataset_id = wait_for_flow_run(
#             run_materializacao_new_dataset_id,
#             stream_states=True,
#             stream_logs=True,
#             raise_final_state=True,
#         )

# gtfs_captura_tratamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# gtfs_captura_tratamento.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value,
#     labels=[constants.RJ_SMTR_AGENT_LABEL.value],
# )
# gtfs_captura_tratamento.state_handlers = [
#     handler_inject_bd_credentials,
#     handler_initialize_sentry,
# ]
