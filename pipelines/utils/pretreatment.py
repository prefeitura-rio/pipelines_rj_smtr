# -*- coding: utf-8 -*-
"""Functions to pretreat data"""
import inspect
from datetime import datetime

import pandas as pd


def transform_to_nested_structure(data: pd.DataFrame, primary_key: list) -> pd.DataFrame:
    """
    Transform columns to nested dict

    Args:
        data (pd.DataFrame): Dataframe to transform
        primary_key (list): List of primary keys

    Returns:
        pd.DataFrame: Nested Dataframe
    """
    return (
        data.groupby(primary_key)
        .apply(lambda x: x[data.columns.difference(primary_key)].to_json(orient="records"))
        .str.strip("[]")
        .reset_index(name="content")[primary_key + ["content"]]
    )


def pretreatment_step(func):
    """Decorator to help develop pretreatment steps"""

    def wrapper(**kwargs):
        signature = inspect.signature(func)
        assert issubclass(
            signature.return_annotation,
            pd.DataFrame,
        ), "return must be pandas DataFrame"
        func_parameter_names = signature.parameters.keys()
        func_parameters = signature.parameters.values()
        expected_arguments = {"data": pd.DataFrame, "timestamp": datetime, "primary_key": list}

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


@pretreatment_step
def strip_string_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    Apply strip function to all string columns in a dataframe

    Args:
        data (pd.DataFrame): Dataframe to treat

    Returns:
        pd.DataFrame: Treated Dataframe
    """
    for col in data.columns[data.dtypes == "object"].to_list():
        if not data[col].isnull().all():
            data[col] = data[col].str.strip()
    return data
