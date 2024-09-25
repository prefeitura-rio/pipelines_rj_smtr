# -*- coding: utf-8 -*-
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_bilhetagem_15min.constants import constants
from pipelines.migration.flows import default_materialization_flow
from pipelines.migration.utils import set_default_parameters
from pipelines.schedules import every_15_minutes

bilhetagem_15min_materializacao = deepcopy(default_materialization_flow)
bilhetagem_15min_materializacao.name = "SMTR: Bilhetagem 15 Minutos - Materialização"
bilhetagem_15min_materializacao.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_15min_materializacao.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_15min_materializacao.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

bilhetagem_15min_materializacao = set_default_parameters(
    flow=bilhetagem_15min_materializacao,
    default_parameters=constants.MATERIALIZACAO_TRANSACAO_15MIN_PARAMS.value,
)

bilhetagem_15min_materializacao.schedule = every_15_minutes
