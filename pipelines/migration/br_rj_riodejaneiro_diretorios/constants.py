# -*- coding: utf-8 -*-
"""
Valores constantes para pipelines br_rj_riodejaneiro_diretorios
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para pipelines br_rj_riodejaneiro_diretorios
    """

    DIRETORIO_MATERIALIZACAO_PARAMS = {
        "dataset_id": "cadastro",
        "upstream": True,
    }

    DIRETORIO_MATERIALIZACAO_TABLE_PARAMS = [
        {"table_id": "diretorio_consorcios"},
        {"table_id": "operadoras_contatos"},
    ]
