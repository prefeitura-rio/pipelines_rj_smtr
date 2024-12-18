# -*- coding: utf-8 -*-
"""
Flows de tratamento do datario
"""


from pipelines.constants import constants as smtr_constants
from pipelines.treatment.datario.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

VIAGEM_INFORMADA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="datario - materializacao",
    selector=constants.DATARIO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    generate_schedule=False,
)
