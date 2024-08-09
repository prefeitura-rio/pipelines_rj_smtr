# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_viagem_conecta

DBT: 2024-08-09
"""
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_viagem_conecta.constants import constants
from pipelines.migration.flows import default_capture_flow, default_materialization_flow
from pipelines.migration.utils import set_default_parameters
from pipelines.schedules import (
    every_day_hour_eleven,
    every_day_hour_ten_minute_five,
    every_day_hour_ten_thirty,
)

# Flows #

viagens_conecta_captura = deepcopy(default_capture_flow)
viagens_conecta_captura.name = "SMTR: Viagens Ônibus Conecta - Captura"
viagens_conecta_captura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
viagens_conecta_captura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
viagens_conecta_captura.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]

viagens_conecta_captura = set_default_parameters(
    flow=viagens_conecta_captura,
    default_parameters=constants.VIAGEM_CAPTURE_PARAMETERS.value,
)

viagens_conecta_captura.schedule = every_day_hour_ten_minute_five


viagens_conecta_recaptura = deepcopy(default_capture_flow)
viagens_conecta_recaptura.name = "SMTR: Viagens Ônibus Conecta - Recaptura"
viagens_conecta_recaptura.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
viagens_conecta_recaptura.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
viagens_conecta_recaptura.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

viagens_conecta_recaptura = set_default_parameters(
    flow=viagens_conecta_recaptura,
    default_parameters=constants.VIAGEM_CAPTURE_PARAMETERS.value | {"recapture": True},
)

viagens_conecta_recaptura.schedule = every_day_hour_ten_thirty


viagem_conecta_materializacao = deepcopy(default_materialization_flow)
viagem_conecta_materializacao.name = "Viagem Conecta - Materialização"
viagem_conecta_materializacao.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
viagem_conecta_materializacao.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)

viagem_conecta_materializacao.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]

viagem_conecta_materializacao = set_default_parameters(
    flow=viagem_conecta_materializacao,
    default_parameters=constants.VIAGEM_MATERIALIZACAO_PARAMS.value,
)

viagem_conecta_materializacao.schedule = every_day_hour_eleven
