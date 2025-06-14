# -*- coding: utf-8 -*-
"""Functions to pretreat data"""
import re

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
