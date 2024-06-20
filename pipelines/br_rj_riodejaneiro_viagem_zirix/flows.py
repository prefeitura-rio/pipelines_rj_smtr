# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_viagem_zirix
"""
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.br_rj_riodejaneiro_viagem_zirix.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import every_10_minutes, every_hour
from pipelines.utils.backup.flows import default_capture_flow
from pipelines.utils.backup.utils import set_default_parameters

# Flows #

viagens_captura = deepcopy(default_capture_flow)
viagens_captura.name = "SMTR: Viagens Ônibus Zirix - Captura"
viagens_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
viagens_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
viagens_captura.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

viagens_captura = set_default_parameters(
    flow=viagens_captura,
    default_parameters=constants.VIAGEM_CAPTURE_PARAMETERS.value,
)

viagens_captura.schedule = every_10_minutes


viagens_recaptura = deepcopy(default_capture_flow)
viagens_recaptura.name = "SMTR: Viagens Ônibus Zirix - Recaptura"
viagens_recaptura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
viagens_recaptura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
viagens_recaptura.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

viagens_recaptura = set_default_parameters(
    flow=viagens_recaptura,
    default_parameters=constants.VIAGEM_CAPTURE_PARAMETERS.value | {"recapture": True},
)

viagens_recaptura.schedule = every_hour
