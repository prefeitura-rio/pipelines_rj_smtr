# -*- coding: utf-8 -*-
import os

# from datetime import datetime as dt
# from datetime import timedelta
from typing import Dict, List, Union

import requests

# import pandas as pd


def run_dbt_model(
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
):
    """
    Run a DBT model.
    """
    run_command = "dbt run"

    common_flags = "-x --profiles-dir ./dev"

    if not flags:
        flags = common_flags
    else:
        flags = common_flags + " " + flags

    if not model:
        model = f"{dataset_id}"
        if table_id:
            model += f".{table_id}"

    # Set models and upstream/downstream for dbt
    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += f"{model}"
        if downstream:
            run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"
    if flags:
        run_command += f" {flags}"

    print(f"\n>>> RUNNING: {run_command}\n")
    os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    os.system(run_command)


def fetch_dataset_sha(dataset_id: str):
    """Fetches the SHA of a branch from Github"""
    url = "https://api.github.com/repos/prefeitura-rio/queries-rj-smtr"
    url += f"/commits?queries-rj-smtr/rj_smtr/{dataset_id}"
    response = requests.get(url)

    if response.status_code != 200:
        return None

    dataset_version = response.json()[0]["sha"]
    return {"version": dataset_version}


def run_dbt_tests(
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
):
    """
    Run DBT test
    """
    run_command = "dbt test"

    common_flags = "--profiles-dir ./dev"

    if flags:
        flags = f"{common_flags} {flags}"
    else:
        flags = common_flags

    if not model:
        model = dataset_id
        if table_id:
            model += f".{table_id}"

    if model:
        run_command += f" --select "
        if upstream:
            run_command += "+"
        run_command += model
        if downstream:
            run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    print(f"\n>>> RUNNING: {run_command}\n")

    project_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    os.chdir(project_dir)
    os.system(run_command)
