# -*- coding: utf-8 -*-
"""Flows de tratamento dos dados de monitoramento"""

from pipelines.capture.rioonibus.constants import (
    constants as rioonibus_source_constants,
)
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.monitoramento.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

VIAGEM_INFORMADA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="viagem_informada - materializacao",
    selector=constants.VIAGEM_INFORMADA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[rioonibus_source_constants.VIAGEM_INFORMADA_SOURCE.value],
)
