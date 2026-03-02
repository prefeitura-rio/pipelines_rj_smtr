# -*- coding: utf-8 -*-
"""
Valores constantes para os processos manuais de bilhetagem
"""

from enum import Enum

from pipelines.capture.jae.constants import CLIENTE_TABLE_ID
from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.capture.jae.flows import (
    CAPTURA_AUXILIAR,
    CAPTURA_GPS_VALIDADOR,
    CAPTURA_LANCAMENTO,
    CAPTURA_TRANSACAO,
    CAPTURA_TRANSACAO_RIOCARD,
)
from pipelines.treatment.bilhetagem.constants import constants as bilhetagem_constants
from pipelines.treatment.bilhetagem.flows import (
    EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO,
    TRANSACAO_MATERIALIZACAO,
)
from pipelines.treatment.cadastro.constants import constants as cadastro_constants
from pipelines.treatment.cadastro.flows import CADASTRO_MATERIALIZACAO
from pipelines.treatment.monitoramento.constants import (
    constants as monitoramento_constants,
)
from pipelines.treatment.monitoramento.flows import GPS_VALIDADOR_MATERIALIZACAO


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para os processos manuais de bilhetagem
    """

    CAPTURE_GAP_TABLES = {
        jae_constants.TRANSACAO_TABLE_ID.value: {
            "flow_name": CAPTURA_TRANSACAO.name,
            "reprocess_all": False,
        },
        jae_constants.TRANSACAO_RIOCARD_TABLE_ID.value: {
            "flow_name": CAPTURA_TRANSACAO_RIOCARD.name,
            "reprocess_all": False,
        },
        jae_constants.GPS_VALIDADOR_TABLE_ID.value: {
            "flow_name": CAPTURA_GPS_VALIDADOR.name,
            "reprocess_all": False,
        },
        jae_constants.LANCAMENTO_TABLE_ID.value: {
            "flow_name": CAPTURA_LANCAMENTO.name,
            "reprocess_all": False,
        },
        CLIENTE_TABLE_ID: {"flow_name": CAPTURA_AUXILIAR.name, "reprocess_all": True},
    }

    CAPTURE_GAP_SELECTORS = {
        cadastro_constants.CADASTRO_SELECTOR.value.name: {
            "flow_name": CADASTRO_MATERIALIZACAO.name,
            "capture_tables": [CLIENTE_TABLE_ID],
            "selector": cadastro_constants.CADASTRO_SELECTOR.value,
        },
        bilhetagem_constants.TRANSACAO_SELECTOR.value.name: {
            "flow_name": TRANSACAO_MATERIALIZACAO.name,
            "capture_tables": [
                jae_constants.TRANSACAO_TABLE_ID.value,
                jae_constants.TRANSACAO_RIOCARD_TABLE_ID.value,
                CLIENTE_TABLE_ID,
            ],
            "selector": bilhetagem_constants.TRANSACAO_SELECTOR.value,
        },
        monitoramento_constants.GPS_VALIDADOR_SELECTOR.value.name: {
            "flow_name": GPS_VALIDADOR_MATERIALIZACAO.name,
            "capture_tables": [jae_constants.GPS_VALIDADOR_TABLE_ID.value],
            "selector": monitoramento_constants.GPS_VALIDADOR_SELECTOR.value,
        },
        bilhetagem_constants.EXTRATO_CLIENTE_CARTAO_SELECTOR.value.name: {
            "flow_name": EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO.name,
            "capture_tables": [jae_constants.LANCAMENTO_TABLE_ID.value, CLIENTE_TABLE_ID],
            "selector": bilhetagem_constants.EXTRATO_CLIENTE_CARTAO_SELECTOR.value,
        },
    }
