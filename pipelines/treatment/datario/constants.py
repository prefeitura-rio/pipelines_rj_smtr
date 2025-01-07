# -*- coding: utf-8 -*-
"""
Valores constantes para materialização dos dados de monitoramento
"""

from datetime import datetime
from enum import Enum

from pipelines.treatment.templates.utils import DBTSelector


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para materialização do datario
    """

    DATARIO_SELECTOR = DBTSelector(
        name="datario",
        initial_datetime=datetime(2024, 12, 16),
    )
