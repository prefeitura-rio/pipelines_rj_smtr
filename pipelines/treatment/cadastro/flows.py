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
        s
        for s in jae_constants.JAE_AUXILIAR_SOURCES.value
        if s.table_id
        in [
            "linha",
            "linha_sem_ressarcimento",
            "linha_consorcio",
            "linha_consorcio_operadora_transporte",
            "cliente",
        ]
    ],
)
