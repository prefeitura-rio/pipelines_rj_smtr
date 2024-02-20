# -*- coding: utf-8 -*-
"""General purpose functions"""
import io
from datetime import date, datetime
from typing import Any

import basedosdados as bd
import pandas as pd
import pytz
from pandas_gbq.exceptions import GenericGBQException
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants

# Set BD config to run on cloud #
bd.config.from_file = True


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


def create_sql_update_filter(
    env: str,
    dataset_id: str,
    table_id: str,
    columns_to_search: list[str],
) -> str:
    """
    Cria condição para ser usada no WHERE de queries SQL
    de modo a buscar por mudanças em um conjunto de colunas
    com base na tabela do BQ.

    Args:
        env (str): Dev ou prod.
        dataset_id (str): Dataset_id no BigQuery.
        table_id (str): Table_id no BigQuery.
        columns_to_search (list[str]): Lista de nomes das colunas
            para buscar por alterações.

    Returns:
        str: Condição para ser adicionada na query. Se a tabela não existir no BQ, retorna 1=1
    """
    project = constants.PROJECT_NAME.value[env]
    log(f"project = {project}")
    columns_to_concat_bq = [c.split(".")[-1] for c in columns_to_search]
    concat_arg = ",'_',"

    try:
        query = f"""
        SELECT
            CONCAT("'", {concat_arg.join(columns_to_concat_bq)}, "'")
        FROM
            `{project}.{dataset_id}_staging.{table_id}`
        """
        log(query)
        last_values = bd.read_sql(query=query, billing_project_id=project)

        last_values = last_values.iloc[:, 0].to_list()
        last_values = ", ".join(last_values)
        update_condition = f"""CONCAT(
                {concat_arg.join(columns_to_search)}
            ) NOT IN ({last_values})
        """

    except GenericGBQException as err:
        if "404 Not found" in str(err):
            log("table not found, setting updates to 1=1")
            update_condition = "1=1"

    return update_condition
