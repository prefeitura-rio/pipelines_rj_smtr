# -*- coding: utf-8 -*-
from prefect.run_configs import KubernetesRun
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.capture.serpro.constants import constants
from pipelines.capture.serpro.tasks import create_serpro_extractor
from pipelines.capture.serpro.utils import handler_setup_serpro
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_SERPRO = create_default_capture_flow(
    flow_name="SMTR: SERPRO - Captura",
    source=constants.AUTUACAO_SOURCE.value,
    create_extractor_task=create_serpro_extractor,
    generate_schedule=False,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
set_default_parameters(CAPTURA_SERPRO, {"recapture": True})

CAPTURA_SERPRO.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE_FEDORA.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
CAPTURA_SERPRO.state_handlers = [
    handler_setup_serpro,
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
# trigger register flow
