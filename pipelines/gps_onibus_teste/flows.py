# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_onibus_gps
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants as emd_constants
from pipelines.gps_onibus_teste.constants import constants
from pipelines.gps_onibus_teste.tasks import (
    create_api_url_onibus_gps,
    create_api_url_onibus_realocacao,
    pre_treatment_br_rj_riodejaneiro_onibus_gps,
    pre_treatment_br_rj_riodejaneiro_onibus_realocacao,
)
from pipelines.schedules import every_10_minutes, every_minute
from pipelines.utils.backup.tasks import (
    bq_upload,
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_raw,
    parse_timestamp_to_string,
    rename_current_flow_run_now_time,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
)

# EMD Imports #


# SMTR Imports #


# Flows #

with Flow(
    "GPS Ônibus Realocação - Captura (teste)",
) as realocacao_sppo:

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
realocacao_sppo.state_handlers = [handler_inject_bd_credentials]
realocacao_sppo.schedule = every_10_minutes


with Flow("GPS Ônibus - Captura (teste)") as captura_sppo_v2:
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

    url = create_api_url_onibus_gps(timestamp=timestamp)

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
captura_sppo_v2.state_handlers = [handler_inject_bd_credentials]
captura_sppo_v2.schedule = every_minute
