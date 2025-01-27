# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de cadastro
"""

from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.cadastro.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

CADASTRO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="cadastro - materializacao",
    selector=constants.CADASTRO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        jae_constants.JAE_AUXILIAR_SOURCES.value["linha"],
        jae_constants.JAE_AUXILIAR_SOURCES.value["operadora_transporte"],
        jae_constants.JAE_AUXILIAR_SOURCES.value["pessoa_fisica"],
        jae_constants.JAE_AUXILIAR_SOURCES.value["consorcio"],
        jae_constants.JAE_AUXILIAR_SOURCES.value["linha_consorcio_operadora_transporte"],
    ],
)
