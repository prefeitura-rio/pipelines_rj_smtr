# -*- coding: utf-8 -*-
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# isort: off
# EMD Imports #

from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_initialize_sentry

from pipelines.constants import constants as emd_constants
from pipelines.schedules import every_minute
from pipelines.test.tasks import test_raise_errors
from pipelines.utils.backup.tasks import get_current_timestamp

with Flow("SMTR - Teste de Erros do Glitch Tip") as raise_flow:
    datetime = get_current_timestamp()
    test_raise_errors(datetime=datetime)

raise_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
raise_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
raise_flow.state_handlers = [handler_initialize_sentry]
raise_flow.schedule = every_minute
