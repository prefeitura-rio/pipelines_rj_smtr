# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados de fiscalização de veiculos
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.utils.gcp.bigquery import SourceTable


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados de fiscalização de veiculos
    """

    FISCALIZACAO_VEICULO_SOURCE_NAME = "fiscalizacao_veiculo"
    VEICULO_LACRE_SHEET_ID = "1LTyNe2_AgWR0JlCmUOYGtYKpe33w57hslkMkrUqYPbw"
    VEICULO_LACRE_SHEET_NAME = "Controle de processos - Fiscalização"
    VEICULO_LACRE_TABLE_ID = "veiculo_fiscalizacao_lacre"
    VEICULO_LACRE_SOURCE = SourceTable(
        source_name=FISCALIZACAO_VEICULO_SOURCE_NAME,
        table_id=VEICULO_LACRE_TABLE_ID,
        first_timestamp=datetime(2025, 5, 27, 5, 0, 0),
        schedule_cron=create_daily_cron(hour=5),
        partition_date_only=True,
        max_recaptures=5,
        primary_keys=["placa", "n_o_de_ordem", "data_do_lacre"],
        raw_filetype="csv",
    )
