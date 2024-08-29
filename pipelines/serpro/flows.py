from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import constants as smtr_constants
from pipelines.serpro.tasks import wait_sleeping

with Flow('SMTR - Teste Conexão Serpro') as flow:
    wait_sleeping()

flow.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE_FEDORA.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)