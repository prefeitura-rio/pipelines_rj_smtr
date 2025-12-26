# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de trânsito

DBT 2025-04-10
"""

from pipelines.capture.serpro.constants import constants as serpro_source_constants
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.treatment.transito.constants import constants

TRANSITO_AUTUACAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="transito_autuacao - materializacao",
    selector=constants.TRANSITO_AUTUACAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    generate_schedule=False,
    snapshot_selector=constants.SNAPSHOT_TRANSITO_SELECTOR.value,
    wait=[serpro_source_constants.AUTUACAO_SOURCE.value],
)
