# -*- coding: utf-8 -*-
# pylint: disable=W0511, E1101
"""
Flows for projeto_subsidio_sppo

DBT  2025-12-26

"""

from datetime import datetime

from prefect import Parameter, case, task
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefect.tasks.core.constants import Constant
from prefect.tasks.core.operators import GreaterThanOrEqual
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow

# from prefeitura_rio.pipelines_utils.prefect import get_flow_run_mode
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.capture.jae.tasks import (
    create_capture_check_discord_message,
    get_capture_gaps,
    jae_capture_check_get_ts_range,
)
from pipelines.constants import constants as smtr_constants
from pipelines.migration.projeto_subsidio_sppo.constants import constants
from pipelines.migration.projeto_subsidio_sppo.tasks import check_param
from pipelines.migration.tasks import (
    check_date_in_range,
    fetch_dataset_sha,
    get_current_flow_labels,
    get_current_flow_mode,
    get_flow_project,
    get_join_dict,
    get_now_date,
    get_now_time,
    get_posterior_date,
    get_previous_date,
    get_run_dates,
    rename_current_flow_run_now_time,
    split_date_range,
)
from pipelines.migration.veiculo.flows import sppo_veiculo_dia
from pipelines.schedules import (
    every_day_hour_five_and_hour_fourteen,
    every_day_hour_seven_minute_five,
)
from pipelines.tasks import (
    add_days_to_date,
    check_fail,
    get_run_env,
    get_scheduled_timestamp,
    log_discord,
    transform_task_state,
)
from pipelines.treatment.templates.tasks import dbt_data_quality_checks, run_dbt

# from pipelines.materialize_to_datario.flows import (
#     smtr_materialize_to_datario_viagem_sppo_flow,
# )

# EMD Imports #


# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

# Flows #

with Flow(
    "SMTR: Viagens SPPO - Tratamento",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as viagens_sppo:
    # Rename flow run
    current_date = get_now_date()

    # Get default parameters #
    date_range_start = Parameter("date_range_start", default=False)
    date_range_end = Parameter("date_range_end", default=False)
    run_d0_param = Parameter("run_d0", default=True)
    rematerialization = Parameter("rematerialization", default=False)

    current_time = get_now_time()
    current_time_formated = task(lambda s: s if len(s) >= 5 else f"0{s}")(current_time)
    second_run = current_time_formated >= "13:59"

    with case(rematerialization, True):
        run_dates_remat = get_run_dates(date_range_start, date_range_end)

    with case(rematerialization, False):
        with case(second_run, True):
            run_dates_true = [{"run_date": get_posterior_date(1)}]

        with case(second_run, False):
            run_dates_false = get_run_dates(date_range_start, date_range_end)

        run_dates_non_remat = merge(run_dates_true, run_dates_false)

    run_dates = merge(run_dates_remat, run_dates_non_remat)

    run_d0_force_false = None
    run_d0_keep_original = run_d0_param

    with case(rematerialization, False):
        with case(second_run, True):
            run_d0_force_false = False

    run_d0 = merge(run_d0_keep_original, run_d0_force_false)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix=viagens_sppo.name + ": ", now_time=run_dates
    )

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()

    # Set dbt client #
    # dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    dataset_sha = fetch_dataset_sha(
        dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
    )

    _vars = get_join_dict(dict_list=run_dates, new_dict=dataset_sha)

    RUN = run_dbt.map(
        resource=unmapped("model"),
        # dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(constants.SUBSIDIO_SPPO_DATASET_ID.value),
        table_id=unmapped(constants.SUBSIDIO_SPPO_TABLE_ID.value),
        upstream=unmapped(True),
        exclude=unmapped("+gps_sppo +ordem_servico_trips_shapes_gtfs"),
        _vars=_vars,
    )

    with case(run_d0, True):
        date_d0 = get_posterior_date(1)
        RUN_2_TRUE = run_dbt(
            resource="model",
            dataset_id=constants.SUBSIDIO_SPPO_DATASET_ID.value,
            table_id="subsidio_data_versao_efetiva viagem_planejada",
            _vars={"run_date": date_d0, "version": dataset_sha},
        )
    with case(run_d0, False):
        RUN_2_FALSE = Constant(value=None, name="RUN_2_FALSE")

    RUN_2 = merge(RUN_2_TRUE, RUN_2_FALSE)

    with case(second_run, False):
        RUN_SNAPSHOTS = run_dbt(
            resource="snapshot",
            selector_name="snapshot_viagem",
            upstream_tasks=[RUN_2],
        )


viagens_sppo.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
viagens_sppo.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value, labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value]
)
viagens_sppo.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
viagens_sppo.schedule = every_day_hour_five_and_hour_fourteen

with Flow(
    "SMTR: Subsídio SPPO Apuração - Tratamento",
    # code_owners=smtr_constants.SUBSIDIO_SPPO_CODE_OWNERS.value,
) as subsidio_sppo_apuracao:
    # 1. SETUP #

    # Get default parameters #
    start_date_param = Parameter("start_date", default=None)
    end_date_param = Parameter("end_date", default=None)

    start_date_cond = check_param(start_date_param)

    with case(start_date_cond, True):
        start_date_get = get_previous_date(7)

    with case(start_date_cond, False):
        start_date_def = start_date_param

    start_date = merge(start_date_get, start_date_def)

    end_date_cond = check_param(end_date_param)

    with case(end_date_cond, True):
        end_date_get = get_previous_date(7)

    with case(end_date_cond, False):
        end_date_def = end_date_param

    end_date = merge(end_date_get, end_date_def)

    materialize_sppo_veiculo_dia = Parameter("materialize_sppo_veiculo_dia", False)
    test_only = Parameter("test_only", False)
    skip_pre_test = Parameter("skip_pre_test", default=False)
    table_ids_jae = Parameter(
        name="table_ids_jae",
        default=["transacao", "transacao_riocard", "gps_validador"],
    )

    # publish = Parameter("publish", False)
    run_dates = get_run_dates(start_date, end_date)
    partitions = task(
        lambda run_dates: ", ".join(
            f"date({dt.year}, {dt.month}, {dt.day})"
            for dt in [datetime.strptime(d["run_date"], "%Y-%m-%d") for d in run_dates]
        )
    )(run_dates)
    dbt_vars = {
        "date_range_start": start_date + "T00:00:00",
        "date_range_end": end_date + "T23:59:59",
        "partitions": partitions,
    }

    # Rename flow run #
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=subsidio_sppo_apuracao.name + ": ", now_time=run_dates
    )

    # Set dbt client #
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode()
    PROJECT = get_flow_project()

    # dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Get models version #
    dataset_sha = fetch_dataset_sha(
        dataset_id=constants.SUBSIDIO_SPPO_DASHBOARD_DATASET_ID.value,
    )

    dates = [{"start_date": start_date, "end_date": end_date}]
    _vars = get_join_dict(dict_list=dates, new_dict=dataset_sha)[0]

    timestamp = get_scheduled_timestamp()
    env = get_run_env()

    # 2. MATERIALIZE DATA #
    with case(test_only, False):
        with case(materialize_sppo_veiculo_dia, True):
            parameters = {
                "start_date": start_date,
                "end_date": end_date,
            }

            SPPO_VEICULO_DIA_RUN = create_flow_run(
                flow_name=sppo_veiculo_dia.name,
                project_name=PROJECT,
                run_name=sppo_veiculo_dia.name,
                parameters=parameters,
            )

            SPPO_VEICULO_DIA_RUN_WAIT_TRUE = wait_for_flow_run(
                SPPO_VEICULO_DIA_RUN,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

        with case(materialize_sppo_veiculo_dia, False):
            SPPO_VEICULO_DIA_RUN_WAIT_FALSE = task(
                lambda: [None], checkpoint=False, name="assign_none_to_previous_runs"
            )()

        SPPO_VEICULO_DIA_RUN_WAIT = merge(
            SPPO_VEICULO_DIA_RUN_WAIT_TRUE, SPPO_VEICULO_DIA_RUN_WAIT_FALSE
        )

        SPPO_VEICULO_DIA_RUN_WAIT = transform_task_state(SPPO_VEICULO_DIA_RUN_WAIT)

        # 3. PRE-DATA QUALITY CHECK #
        with case(skip_pre_test, False):
            timestamp_captura_start, timestamp_captura_end = jae_capture_check_get_ts_range(
                timestamp=timestamp,
                retroactive_days=7,
                timestamp_captura_start=start_date_param,
                timestamp_captura_end=add_days_to_date(date_str=end_date_param, days=7),
            ).set_upstream(task=SPPO_VEICULO_DIA_RUN_WAIT)

            timestamps = get_capture_gaps.map(
                env=unmapped(env),
                table_id=table_ids_jae,
                timestamp_captura_start=unmapped(timestamp_captura_start),
                timestamp_captura_end=unmapped(timestamp_captura_end),
            )

            discord_messages = create_capture_check_discord_message.map(
                table_id=table_ids_jae,
                timestamps=timestamps,
                timestamp_captura_start=unmapped(timestamp_captura_start),
                timestamp_captura_end=unmapped(timestamp_captura_end),
            )

            send_discord_message = log_discord.map(
                message=discord_messages,
                key=unmapped("subsidio_data_check"),
            )

            missing_timestamps = task(
                lambda timestamps_list: any(len(ts) > 0 for ts in timestamps_list)
            )(timestamps)

            SUBSIDIO_SPPO_DATA_QUALITY_PRE = run_dbt(
                resource="test",
                dataset_id=constants.SUBSIDIO_SPPO_PRE_TEST.value,
                exclude="dashboard_subsidio_sppo_v2 teto_viagens__viagens_remuneradas not_null__data_ordem__transacao sincronizacao_tabelas__transacao_gratuidade_estudante_municipal",  # noqa
                _vars=dbt_vars,
                upstream_tasks=[timestamps],
            )

            DATA_QUALITY_PRE = dbt_data_quality_checks(
                dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_PRE,
                checks_list=constants.SUBSIDIO_SPPO_PRE_CHECKS_LIST.value,
                webhook_key="subsidio_data_check",
                params=dbt_vars,
            )

            test_failed = check_fail(DATA_QUALITY_PRE)
            skip_materialization_check = task(lambda a, b: a or b)(missing_timestamps, test_failed)

        with case(skip_pre_test, True):
            skip_materialization_false = False

        with case(test_only, True):
            skip_materialization = True

        with case(test_only, False):
            skip_materialization = merge(skip_materialization_check, skip_materialization_false)

        with case(skip_materialization, False):
            # 4. CALCULATE #
            date_in_range = check_date_in_range(
                _vars["start_date"], _vars["end_date"], constants.DATA_SUBSIDIO_V9_INICIO.value
            )

            with case(date_in_range, True):
                date_intervals = split_date_range(
                    _vars["start_date"], _vars["end_date"], constants.DATA_SUBSIDIO_V9_INICIO.value
                )

                dbt_vars_first_range = get_join_dict(
                    dict_list=[_vars], new_dict=date_intervals["first_range"]
                )[0]

                APURACAO_FIRST_RANGE_RUN = run_dbt(
                    resource="model",
                    selector_name="apuracao_subsidio_v8",
                    _vars=dbt_vars_first_range,
                )

                # POST-DATA QUALITY CHECK #
                DATA_QUALITY_POS_FIRST_RANGE = run_dbt(
                    resource="test",
                    dataset_id="dashboard_subsidio_sppo",
                    _vars={
                        "date_range_start": date_intervals["first_range"]["start_date"],
                        "date_range_end": date_intervals["first_range"]["end_date"],
                    },
                    upstream_tasks=[APURACAO_FIRST_RANGE_RUN],
                )

                dbt_data_quality_checks(
                    dbt_logs=DATA_QUALITY_POS_FIRST_RANGE,
                    checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                    webhook_key="subsidio_data_check",
                    params={
                        "date_range_start": date_intervals["first_range"]["start_date"],
                        "date_range_end": date_intervals["first_range"]["end_date"],
                    },
                )

                dbt_vars_second_range = get_join_dict(
                    dict_list=[dbt_vars_first_range],
                    new_dict=date_intervals["second_range"],
                    upstream_tasks=[DATA_QUALITY_POS_FIRST_RANGE],
                )[0]

                APURACAO_SECOND_RANGE = run_dbt(
                    resource="model",
                    selector_name="apuracao_subsidio_v9",
                    _vars=dbt_vars_second_range,
                    upstream_tasks=[dbt_vars_second_range],
                )
                dbt_vars_monitoramento = get_join_dict(
                    dict_list=[dbt_vars_second_range],
                    new_dict={"tipo_materializacao": "monitoramento"},
                    upstream_tasks=[APURACAO_SECOND_RANGE],
                )[0]

                MONITORAMENTO_RUN = run_dbt(
                    resource="model",
                    selector_name="monitoramento_subsidio",
                    _vars=dbt_vars_monitoramento,
                    upstream_tasks=[dbt_vars_monitoramento],
                )

                # POST-DATA QUALITY CHECK #
                DATA_QUALITY_POS_SECOND_RANGE = run_dbt(
                    resource="test",
                    dataset_id=constants.SUBSIDIO_SPPO_V9_POS_CHECKS_DATASET_ID.value,
                    _vars={
                        "date_range_start": date_intervals["second_range"]["start_date"],
                        "date_range_end": date_intervals["second_range"]["end_date"],
                    },
                ).set_upstream(task=APURACAO_SECOND_RANGE)

                dbt_data_quality_checks(
                    dbt_logs=DATA_QUALITY_POS_SECOND_RANGE,
                    checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                    webhook_key="subsidio_data_check",
                    params={
                        "date_range_start": date_intervals["second_range"]["start_date"],
                        "date_range_end": date_intervals["second_range"]["end_date"],
                    },
                )

            with case(date_in_range, False):
                gte = GreaterThanOrEqual()
                data_maior_ou_igual_v9 = gte.run(
                    _vars["start_date"], constants.DATA_SUBSIDIO_V9_INICIO.value
                )

                with case(data_maior_ou_igual_v9, False):
                    APURACAO_V8_RUN = run_dbt(
                        resource="model",
                        selector_name="apuracao_subsidio_v8",
                        _vars=_vars,
                    )

                    # POST-DATA QUALITY CHECK #
                    DATA_QUALITY_POS_V8 = run_dbt(
                        resource="test",
                        dataset_id="dashboard_subsidio_sppo",
                        _vars=dbt_vars,
                        upstream_tasks=[APURACAO_V8_RUN],
                    )

                    dbt_data_quality_checks(
                        dbt_logs=DATA_QUALITY_POS_V8,
                        checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                        webhook_key="subsidio_data_check",
                        params=dbt_vars,
                    )

                with case(data_maior_ou_igual_v9, True):
                    APURACAO_V9_RUN = run_dbt(
                        resource="model",
                        selector_name="apuracao_subsidio_v9",
                        _vars=_vars,
                    )

                    _vars_v9 = get_join_dict(
                        dict_list=[_vars],
                        new_dict={"tipo_materializacao": "monitoramento"},
                        upstream_tasks=[APURACAO_V9_RUN],
                    )[0]

                    MONITORAMENTO_V9_RUN = run_dbt(
                        resource="model",
                        selector_name="monitoramento_subsidio",
                        _vars=_vars_v9,
                        upstream_tasks=[_vars_v9],
                    )
                    # POST-DATA QUALITY CHECK #
                    date_in_range_v14 = check_date_in_range(
                        _vars["start_date"],
                        _vars["end_date"],
                        constants.DATA_SUBSIDIO_V14_INICIO.value,
                    )

                    with case(date_in_range_v14, True):
                        date_intervals = split_date_range(
                            _vars["start_date"],
                            _vars["end_date"],
                            constants.DATA_SUBSIDIO_V14_INICIO.value,
                        )

                        DATA_QUALITY_POS_V9_FIRST_RANGE = run_dbt(
                            resource="test",
                            dataset_id=constants.SUBSIDIO_SPPO_V9_POS_CHECKS_DATASET_ID.value,  # noqa
                            _vars={
                                "date_range_start": date_intervals["first_range"]["start_date"],
                                "date_range_end": date_intervals["first_range"]["end_date"],
                            },
                            upstream_tasks=[APURACAO_V9_RUN],
                        )

                        dbt_data_quality_checks(
                            dbt_logs=DATA_QUALITY_POS_V9_FIRST_RANGE,
                            checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                            webhook_key="subsidio_data_check",
                            params={
                                "date_range_start": date_intervals["first_range"]["start_date"],
                                "date_range_end": date_intervals["first_range"]["end_date"],
                            },
                        )

                        DATA_QUALITY_POS_V9_SECOND_RANGE = run_dbt(
                            resource="test",
                            dataset_id=constants.SUBSIDIO_SPPO_V14_POS_CHECKS_DATASET_ID.value,  # noqa
                            _vars={
                                "date_range_start": date_intervals["second_range"]["start_date"],
                                "date_range_end": date_intervals["second_range"]["end_date"],
                            },
                            upstream_tasks=[DATA_QUALITY_POS_V9_FIRST_RANGE],
                        )

                        dbt_data_quality_checks(
                            dbt_logs=DATA_QUALITY_POS_V9_SECOND_RANGE,
                            checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                            webhook_key="subsidio_data_check",
                            params={
                                "date_range_start": date_intervals["second_range"]["start_date"],
                                "date_range_end": date_intervals["second_range"]["end_date"],
                            },
                        )
                    with case(date_in_range_v14, False):
                        data_maior_ou_igual_v14 = gte.run(
                            _vars["start_date"], constants.DATA_SUBSIDIO_V14_INICIO.value
                        )

                        with case(data_maior_ou_igual_v14, False):
                            DATA_QUALITY_POS_BEFORE_V14 = run_dbt(
                                resource="test",
                                dataset_id=constants.SUBSIDIO_SPPO_V9_POS_CHECKS_DATASET_ID.value,  # noqa
                                _vars=dbt_vars,
                                upstream_tasks=[APURACAO_V9_RUN],
                            )

                            DATA_QUALITY_POS_BEFORE_V14 = transform_task_state(
                                DATA_QUALITY_POS_BEFORE_V14
                            )

                        with case(data_maior_ou_igual_v14, True):
                            DATA_QUALITY_POS_V14 = run_dbt(
                                resource="test",
                                dataset_id=constants.SUBSIDIO_SPPO_V14_POS_CHECKS_DATASET_ID.value,  # noqa
                                _vars=dbt_vars,
                                upstream_tasks=[APURACAO_V9_RUN],
                            )

                            DATA_QUALITY_POS_V14 = transform_task_state(DATA_QUALITY_POS_V14)

                        DATA_QUALITY_POS = merge(DATA_QUALITY_POS_BEFORE_V14, DATA_QUALITY_POS_V14)

                        dbt_data_quality_checks(
                            dbt_logs=DATA_QUALITY_POS,
                            checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                            webhook_key="subsidio_data_check",
                            params=dbt_vars,
                        )
            RUN_APURACAO_V9 = merge(APURACAO_SECOND_RANGE, APURACAO_V9_RUN)

            RUN_SNAPSHOTS = run_dbt(
                resource="snapshot",
                selector_name="snapshot_subsidio",
                upstream_tasks=[RUN_APURACAO_V9],
            )

            # TODO: test upstream_tasks=[SUBSIDIO_SPPO_DASHBOARD_RUN]
            # 6. PUBLISH #
            # with case(publish, True):

            #     SMTR_MATERIALIZE_TO_DATARIO_VIAGEM_SPPO_RUN = create_flow_run(
            #         flow_name=smtr_materialize_to_datario_viagem_sppo_flow.name,
            #         project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            #         labels=[
            #             constants.RJ_DATARIO_AGENT_LABEL.value,
            #         ],
            #         run_name=smtr_materialize_to_datario_viagem_sppo_flow.name,
            #         parameters={
            #             "dataset_id": "transporte_rodoviario_municipal",
            #             "table_id": "viagem_onibus",
            #             "mode": "prod",
            #             "dbt_model_parameters": _vars,
            #         },
            #         upstream_tasks=[SUBSIDIO_SPPO_DASHBOARD_RUN],
            #     )

            #     wait_for_flow_run(
            #         SMTR_MATERIALIZE_TO_DATARIO_VIAGEM_SPPO_RUN,
            #         stream_states=True,
            #         stream_logs=True,
            #         raise_final_state=True,
            #     )

            #     SMTR_MATERIALIZE_TO_DATARIO_VIAGEM_SPPO_RUN.set_upstream(
            #         SUBSIDIO_SPPO_DASHBOARD_RUN
            #     )
    with case(test_only, True):
        timestamp_captura_start, timestamp_captura_end = jae_capture_check_get_ts_range(
            timestamp=timestamp,
            retroactive_days=7,
            timestamp_captura_start=start_date_param,
            timestamp_captura_end=add_days_to_date(date_str=end_date_param, days=7),
        )

        timestamps = get_capture_gaps.map(
            env=unmapped(env),
            table_id=table_ids_jae,
            timestamp_captura_start=unmapped(timestamp_captura_start),
            timestamp_captura_end=unmapped(timestamp_captura_end),
        )

        discord_messages = create_capture_check_discord_message.map(
            table_id=table_ids_jae,
            timestamps=timestamps,
            timestamp_captura_start=unmapped(timestamp_captura_start),
            timestamp_captura_end=unmapped(timestamp_captura_end),
        )

        send_discord_message = log_discord.map(
            message=discord_messages,
            key=unmapped("subsidio_data_check"),
        )

        SUBSIDIO_SPPO_DATA_QUALITY_PRE = run_dbt(
            resource="test",
            dataset_id=constants.SUBSIDIO_SPPO_PRE_TEST.value,
            exclude="dashboard_subsidio_sppo_v2 teto_viagens__viagens_remuneradas not_null__data_ordem__transacao sincronizacao_tabelas__transacao_gratuidade_estudante_municipal",  # noqa
            _vars=dbt_vars,
        ).set_upstream(task=send_discord_message)

        DATA_QUALITY_PRE = dbt_data_quality_checks(
            dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_PRE,
            checks_list=constants.SUBSIDIO_SPPO_PRE_CHECKS_LIST.value,
            webhook_key="subsidio_data_check",
            params=dbt_vars,
        )

        date_in_range = check_date_in_range(
            _vars["start_date"], _vars["end_date"], constants.DATA_SUBSIDIO_V9_INICIO.value
        )

        with case(date_in_range, True):
            date_intervals = split_date_range(
                _vars["start_date"], _vars["end_date"], constants.DATA_SUBSIDIO_V9_INICIO.value
            )

            SUBSIDIO_SPPO_DATA_QUALITY_POS = run_dbt(
                resource="test",
                dataset_id="dashboard_subsidio_sppo",
                _vars={
                    "date_range_start": date_intervals["first_range"]["start_date"],
                    "date_range_end": date_intervals["first_range"]["end_date"],
                },
            )

            DATA_QUALITY_POS = dbt_data_quality_checks(
                dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_POS,
                checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                webhook_key="subsidio_data_check",
                params={
                    "date_range_start": date_intervals["first_range"]["start_date"],
                    "date_range_end": date_intervals["first_range"]["end_date"],
                },
            )

            SUBSIDIO_SPPO_DATA_QUALITY_POS_2 = run_dbt(
                resource="test",
                dataset_id="viagens_remuneradas sumario_servico_dia_pagamento",
                _vars={
                    "date_range_start": date_intervals["second_range"]["start_date"],
                    "date_range_end": date_intervals["second_range"]["end_date"],
                },
            ).set_upstream(task=SUBSIDIO_SPPO_DATA_QUALITY_POS)

            DATA_QUALITY_POS_2 = dbt_data_quality_checks(
                dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_POS_2,
                checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                webhook_key="subsidio_data_check",
                params={
                    "date_range_start": date_intervals["second_range"]["start_date"],
                    "date_range_end": date_intervals["second_range"]["end_date"],
                },
            )

        with case(date_in_range, False):
            gte = GreaterThanOrEqual()
            data_maior_ou_igual_v9 = gte.run(
                _vars["start_date"], constants.DATA_SUBSIDIO_V9_INICIO.value
            )

            with case(data_maior_ou_igual_v9, False):
                SUBSIDIO_SPPO_DATA_QUALITY_POS = run_dbt(
                    resource="test",
                    dataset_id="dashboard_subsidio_sppo",
                    _vars=dbt_vars,
                )

                DATA_QUALITY_POS = dbt_data_quality_checks(
                    dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_POS,
                    checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                    webhook_key="subsidio_data_check",
                    params=dbt_vars,
                )

            with case(data_maior_ou_igual_v9, True):
                date_in_range = check_date_in_range(
                    _vars["start_date"], _vars["end_date"], constants.DATA_SUBSIDIO_V14_INICIO.value
                )

                with case(date_in_range, True):
                    date_intervals = split_date_range(
                        _vars["start_date"],
                        _vars["end_date"],
                        constants.DATA_SUBSIDIO_V14_INICIO.value,
                    )

                    SUBSIDIO_SPPO_DATA_QUALITY_POS = run_dbt(
                        resource="test",
                        dataset_id=constants.SUBSIDIO_SPPO_V9_POS_CHECKS_DATASET_ID.value,  # noqa
                        _vars={
                            "date_range_start": date_intervals["first_range"]["start_date"],
                            "date_range_end": date_intervals["first_range"]["end_date"],
                        },
                    )

                    DATA_QUALITY_POS = dbt_data_quality_checks(
                        dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_POS,
                        checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                        webhook_key="subsidio_data_check",
                        params={
                            "date_range_start": date_intervals["first_range"]["start_date"],
                            "date_range_end": date_intervals["first_range"]["end_date"],
                        },
                    )

                    SUBSIDIO_SPPO_DATA_QUALITY_POS_2 = run_dbt(
                        resource="test",
                        dataset_id=constants.SUBSIDIO_SPPO_V14_POS_CHECKS_DATASET_ID.value,  # noqa
                        _vars={
                            "date_range_start": date_intervals["second_range"]["start_date"],
                            "date_range_end": date_intervals["second_range"]["end_date"],
                        },
                    ).set_upstream(task=SUBSIDIO_SPPO_DATA_QUALITY_POS)

                    DATA_QUALITY_POS_2 = dbt_data_quality_checks(
                        dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_POS_2,
                        checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                        webhook_key="subsidio_data_check",
                        params={
                            "date_range_start": date_intervals["second_range"]["start_date"],
                            "date_range_end": date_intervals["second_range"]["end_date"],
                        },
                    )
                with case(date_in_range, False):
                    data_maior_ou_igual_v14 = gte.run(
                        _vars["start_date"], constants.DATA_SUBSIDIO_V14_INICIO.value
                    )
                    with case(data_maior_ou_igual_v14, False):
                        SUBSIDIO_SPPO_DATA_QUALITY_POS_V9 = run_dbt(
                            resource="test",
                            dataset_id=constants.SUBSIDIO_SPPO_V9_POS_CHECKS_DATASET_ID.value,  # noqa
                            _vars=dbt_vars,
                        )

                        SUBSIDIO_SPPO_DATA_QUALITY_POS_V9 = transform_task_state(
                            SUBSIDIO_SPPO_DATA_QUALITY_POS_V9
                        )

                    with case(data_maior_ou_igual_v14, True):
                        SUBSIDIO_SPPO_DATA_QUALITY_POS_V14 = run_dbt(
                            resource="test",
                            dataset_id=constants.SUBSIDIO_SPPO_V14_POS_CHECKS_DATASET_ID.value,  # noqa
                            _vars=dbt_vars,
                            exclude="aux_viagem_temperatura veiculo_regularidade_temperatura_dia",
                        )

                        SUBSIDIO_SPPO_DATA_QUALITY_POS_V14 = transform_task_state(
                            SUBSIDIO_SPPO_DATA_QUALITY_POS_V14
                        )

                    SUBSIDIO_SPPO_DATA_QUALITY_POS = merge(
                        SUBSIDIO_SPPO_DATA_QUALITY_POS_V9, SUBSIDIO_SPPO_DATA_QUALITY_POS_V14
                    )

                    DATA_QUALITY_POS = dbt_data_quality_checks(
                        dbt_logs=SUBSIDIO_SPPO_DATA_QUALITY_POS,
                        checks_list=constants.SUBSIDIO_SPPO_POS_CHECKS_LIST.value,
                        webhook_key="subsidio_data_check",
                        params=dbt_vars,
                    )
subsidio_sppo_apuracao.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_apuracao.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value, labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value]
)
subsidio_sppo_apuracao.state_handlers = [handler_initialize_sentry, handler_inject_bd_credentials]
subsidio_sppo_apuracao.schedule = every_day_hour_seven_minute_five
