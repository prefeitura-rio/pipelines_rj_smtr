# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.constants import constants as emd_constants
from pipelines.schedules import every_day_hour_seven
from pipelines.utils.backup.tasks import bq_upload, get_rounded_timestamp
from pipelines.veiculo.tasks import (
    download_and_save_local_from_ftp,
    get_ftp_filepaths,
    pre_treatment_sppo_infracao,
    pre_treatment_sppo_licenciamento,
)

# Flows #

with Flow("SMTR: veiculo sppo_infracao - captura") as captura_stu_ftp:

    timestamp = Parameter("timestamp", default=None)
    search_dir = Parameter("search_dir", default="multas")
    dataset_id = Parameter("dataset_id", default=constants.VEICULO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.SPPO_INFRACAO_TABLE_ID.value)

    # MODE = get_current_flow_mode()
    timestamp = get_rounded_timestamp(timestamp)
    # EXTRACT
    files = get_ftp_filepaths(search_dir=search_dir, timestamp=timestamp)
    # download_files = check_files_for_download(
    #     files=files, dataset_id=dataset_id, table_id=table_id, mode=MODE
    # )
    updated_files_info = download_and_save_local_from_ftp.map(
        file_info=files, dataset_id=dataset_id, table_id=table_id
    )
    # TRANSFORM
    treated_paths, raw_paths, partitions, status = pre_treatment_sppo_infracao(
        files=updated_files_info
    )

    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        filepath=treated_paths,
        raw_filepath=raw_paths,
        partitions=partitions,
        status=status,
    )
    # set_redis = update_redis_ftp_files(
    #     download_files=download_files,
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     errors=errors,
    #     mode=MODE,
    # )

captura_stu_ftp.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_stu_ftp.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_stu_ftp.schedule = every_day_hour_seven
captura_stu_ftp.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]
with Flow("SMTR: veiculo sppo_licenciamento_stu - captura") as captura_licenciamento_ftp:

    timestamp = Parameter("timestamp", default=None)
    search_dir = Parameter("search_dir", default="licenciamento")
    dataset_id = Parameter("dataset_id", default=constants.VEICULO_DATASET_ID.value)
    table_id = Parameter("table_id", default=constants.SPPO_LICENCIAMENTO_TABLE_ID.value)

    timestamp = get_rounded_timestamp(timestamp)
    # EXTRACT
    files = get_ftp_filepaths(search_dir=search_dir, timestamp=timestamp)
    # download_files = check_files_for_download(
    #     files=files, dataset_id=dataset_id, table_id=table_id, mode=MODE
    # )
    updated_files_info = download_and_save_local_from_ftp.map(
        file_info=files, dataset_id=dataset_id, table_id=table_id
    )
    # TRANSFORM
    treated_paths, raw_paths, partitions, status = pre_treatment_sppo_licenciamento(
        files=updated_files_info
    )

    # LOAD
    errors = bq_upload.map(
        dataset_id=unmapped(dataset_id),
        table_id=unmapped(table_id),
        filepath=treated_paths,
        raw_filepath=raw_paths,
        partitions=partitions,
        status=status,
    )
    # set_redis = update_redis_ftp_files(
    #     download_files=download_files,
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     errors=errors,
    #     mode=MODE,
    # )

captura_licenciamento_ftp.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_licenciamento_ftp.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
captura_licenciamento_ftp.schedule = every_day_hour_seven
captura_licenciamento_ftp.state_handlers = [
    handler_initialize_sentry,
    handler_inject_bd_credentials,
]
