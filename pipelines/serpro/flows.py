# -*- coding: utf-8 -*-
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants as smtr_constants
from pipelines.serpro.tasks import wait_sleeping
from pipelines.serpro.utils import handler_setup_serpro

with Flow("SMTR - Teste Conexão Serpro") as flow:
    # setup_serpro()
    wait_sleeping()

flow.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE_FEDORA.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
flow.state_handlers = [handler_setup_serpro, handler_inject_bd_credentials]
