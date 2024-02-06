# -*- coding: utf-8 -*-
"""General purpose functions"""
import io
from datetime import date, datetime
from typing import Any

import pandas as pd
import pytz

from pipelines.constants import constants


def custom_serialization(obj: Any) -> Any:
    """
    Função para serializar objetos não serializaveis
    pela função json.dump

    Args:
        obj (Any): Objeto a ser serializado

    Returns:
        Any: Object serializado
    """
    if isinstance(obj, (pd.Timestamp, date)):
        if isinstance(obj, pd.Timestamp):
            if obj.tzinfo is None:
                obj = obj.tz_localize("UTC").tz_convert(constants.TIMEZONE.value)
        return obj.isoformat()

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def data_info_str(data: pd.DataFrame):
    """
    Retorna as informações de um Dataframe como string

    Args:
        data (pd.DataFrame): Dataframe para extrair as informações

    Returns:
        str: retorno do método data.info()
    """
    buffer = io.StringIO()
    data.info(buf=buffer)
    return buffer.getvalue()


def create_timestamp_captura(timestamp: datetime) -> str:
    """
    Cria o valor para a coluna timestamp_captura

    Args:
        timestamp (datetime): timestamp a ser escrita

    Returns:
        str: Valor a ser escrito na coluna timestamp_captura
    """
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=pytz.UTC)

    return timestamp.astimezone(tz=pytz.timezone(constants.TIMEZONE.value)).strftime(
        "%Y-%m-%d %H:%M:%S-03:00"
    )


def isostr_to_datetime(datetime_str: str) -> datetime:
    """
    Converte uma string de data no formato iso em um datetime em UTC

    Args:
        datetime_str (str): String a ser convertida

    Returns:
        datetime: String convertida em datetime
    """
    converted = datetime.fromisoformat(datetime_str)
    if converted.tzinfo is None:
        converted = converted.replace(tzinfo=pytz.UTC)
    else:
        converted = converted.astimezone(tz=pytz.timezone("UTC"))

    return converted
