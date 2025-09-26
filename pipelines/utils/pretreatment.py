# -*- coding: utf-8 -*-
"""Functions to pretreat data"""
import re
from datetime import datetime
from typing import Callable

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from unidecode import unidecode


def normalize_text(text):
    text = unidecode(text)
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    text = text.strip("_")
    text = text.lower()

    return text


def transform_to_nested_structure(data: pd.DataFrame, primary_keys: list) -> pd.DataFrame:
    """
    Transforma colunas do DataFrame na coluna content no formato Json
    agrupando pelas primary keys

    Args:
        data (pd.DataFrame): DataFrame para aplicar o tratamento
        primary_keys (list): Lista de primary keys

    Returns:
        pd.DataFrame: Dataframe contendo as colunas listadas nas primary keys + coluna content
    """
    content_columns = [c for c in data.columns if c not in primary_keys]
    data["content"] = data.apply(
        lambda row: row[[c for c in content_columns]].to_json(),
        axis=1,
    )
    return data[primary_keys + ["content"]]


def strip_string_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica a função strip em todas as colunas do formato string
    de um DataFrame

    Args:
        data (pd.DataFrame): Dataframe a ser tratado

    Returns:
        pd.DataFrame: Dataframe tratado
    """
    for col in data.columns[data.dtypes == "object"].to_list():
        try:
            data[col] = data[col].str.strip()
        except AttributeError as e:
            log(f"Error {e} on column {col}")
    return data


def raise_if_column_isna(
    column_name: str,
) -> Callable[[pd.DataFrame, datetime, list], pd.DataFrame]:
    """
    Cria uma função de validação que lança um erro caso a coluna especificada contenha valores nulos

    Args:
        column_name (str): Nome da coluna a ser validada

    Returns:
        Callable[[pd.DataFrame, datetime, list], pd.DataFrame]:
        Uma função que recebe um DataFrame, um timestamp e chaves primárias,
        validando a coluna especificada
    """

    def func(data: pd.DataFrame, timestamp: datetime, primary_keys: list):
        if not data[data[column_name].isna()].empty:
            raise ValueError(f"A coluna {column_name} está nula")
        return data

    return func
