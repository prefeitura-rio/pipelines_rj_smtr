# -*- coding: utf-8 -*-
"""Functions to pretreat data"""
import inspect
import json
from datetime import datetime

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log


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

    data["content"] = data.apply(
        lambda row: json.dumps(
            row.drop(columns=primary_keys).to_dict(),
        ),
        axis=1,
    )
    return data[primary_keys + ["content"]]


def pretreatment_func(func):
    """
    Decorator para ajudar no desenvolvimento de funções
    de pre-tratamento para serem passadas no flow generico de captura

    Faz a checagem dos parâmetros e do retorno da função
    e possibilita a criação da função sem precisar de todos
    os parâmetros passados pela Task
    """

    def wrapper(**kwargs):
        signature = inspect.signature(func)
        assert issubclass(
            signature.return_annotation,
            pd.DataFrame,
        ), "return must be pandas DataFrame"
        func_parameter_names = signature.parameters.keys()
        func_parameters = signature.parameters.values()
        expected_arguments = {"data": pd.DataFrame, "timestamp": datetime, "primary_keys": list}

        invalid_args = [
            a.name
            for a in func_parameters
            if a.name not in expected_arguments
            or isinstance(a.annotation, expected_arguments[a.name])
        ]

        if len(invalid_args) > 0:
            raise ValueError(f"Invalid arguments: {', '.join(invalid_args)}")

        kwargs = {k: v for k, v in kwargs.items() if k in func_parameter_names}
        return func(**kwargs)

    return wrapper


@pretreatment_func
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
