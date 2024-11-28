# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants as smtr_constants

# from pipelines.migration.tasks import upload_raw_data_to_gcs
from pipelines.serpro.tasks import dump_serpro, get_db_object, wait_sleeping
from pipelines.serpro.utils import handler_setup_serpro

with Flow("SMTR - Teste Conexão Serpro") as flow:
    batch_size = Parameter("batch_size", default=100000)
    # setup_serpro()

    jdbc = get_db_object()
    csv_files = dump_serpro(jdbc, batch_size)

    wait_sleeping()

    # upload_raw_data_to_gcs.map(
    #     dataset_id=unmapped("radar_serpro"),
    #     table_id=unmapped("tb_infracao_view"),
    #     raw_filepath=csv_files,
    #     partitions=unmapped(None),
    #     error=unmapped(None),
    #     bucket_name=unmapped("rj-smtr-dev"),
    # )

flow.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE_FEDORA.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
flow.state_handlers = [handler_setup_serpro, handler_inject_bd_credentials]
