# -*- coding: utf-8 -*-
"""
Valores constantes para os processos manuais de bilhetagem
"""

from enum import Enum

from pipelines.capture.jae.flows import (
    CAPTURA_GPS_VALIDADOR,
    CAPTURA_LANCAMENTO,
    CAPTURA_TRANSACAO,
    CAPTURA_TRANSACAO_RIOCARD,
)
from pipelines.treatment.bilhetagem.flows import (
    EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO,
    TRANSACAO_MATERIALIZACAO,
)
from pipelines.treatment.monitoramento.flows import GPS_VALIDADOR_MATERIALIZACAO


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para os processos manuais de bilhetagem
    """

    CAPTURE_GAP_TABLES = {
        "transacao": CAPTURA_TRANSACAO.name,
        "transacao_riocard": CAPTURA_TRANSACAO_RIOCARD.name,
        "gps_validador": CAPTURA_GPS_VALIDADOR.name,
        "lancamento": CAPTURA_LANCAMENTO.name,
    }

    CAPTURE_GAP_SELECTORS = {
        "transacao": {
            "flow_name": TRANSACAO_MATERIALIZACAO.name,
            "capture_tables": ["transacao", "transacao_riocard"],
        },
        "gps_validador": {
            "flow_name": GPS_VALIDADOR_MATERIALIZACAO.name,
            "capture_tables": ["gps_validador"],
        },
        "extrato_cliente_cartao": {
            "flow_name": EXTRATO_CLIENTE_CARTAO_MATERIALIZACAO.name,
            "capture_tables": ["lancamento"],
        },
    }
