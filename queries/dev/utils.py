# -*- coding: utf-8 -*-
# flake8: noqa
import os
import re
import subprocess
from typing import Dict, List, Union

import requests
from prefect import context

# from datetime import datetime as dt
# from datetime import timedelta
from pipelines.constants import constants as smtr_constants
from pipelines.utils.discord import format_send_discord_message
from pipelines.utils.secret import get_secret

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
    test_name: str = None,
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
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += model
        if downstream:
            run_command += "+"
        if test_name:
            model += f",test_name:{test_name}"

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
    dbt_logs = subprocess.run(run_command, shell=True, capture_output=True, text=True)

    print(dbt_logs.stdout)
    return dbt_logs.stdout


def parse_dbt_test_output(dbt_logs: str) -> dict:
    # Remover sequências ANSI
    dbt_logs = re.sub(r"\x1B[@-_][0-?]*[ -/]*[@-~]", "", dbt_logs)

    results = {}
    result_pattern = r"\d+ of \d+ (PASS|FAIL|ERROR) (\d+ )?([\w_]+) .* \[(PASS|FAIL|ERROR) .*\]"
    fail_pattern = r"Failure in test ([\w_]+) .*\n.*\n.*\n.* compiled Code at (.*)\n"
    error_pattern = r"Error in test ([\w_]+) \(.*schema.yaml\)\n  (.*)\n"

    for match in re.finditer(result_pattern, dbt_logs):
        groups = match.groups()
        test_name = groups[2]
        results[test_name] = {"result": groups[3]}

    for match in re.finditer(fail_pattern, dbt_logs):
        groups = match.groups()
        test_name = groups[0]
        file = groups[1]

        with open(file, "r") as arquivo:
            query = arquivo.read()

        query = re.sub(r"\n+", "\n", query)
        results[test_name]["query"] = query

    for match in re.finditer(error_pattern, dbt_logs):
        groups = match.groups()
        test_name = groups[0]
        error = groups[1]
        results[test_name]["error"] = error

    log_message = ""
    for test, info in results.items():
        result = info["result"]
        log_message += f"Test: {test} Status: {result}\n"

        if result == "FAIL":
            log_message += "Query:\n"
            log_message += f"{info['query']}\n"

        if result == "ERROR":
            log_message += f"Error: {info['error']}\n"

        log_message += "\n"

    print(log_message)

    return results


def dbt_data_quality_checks(
    checks_list: dict, checks_results: dict, params: dict, webhook_url: str = None
) -> bool:

    if webhook_url is None:
        webhook_url = get_secret(secret_path=smtr_constants.WEBHOOKS_SECRET_PATH.value)["dataplex"]

    dados_tag = f" - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS.value['dados_smtr']['user_id']}>\n"

    test_check = all(test["result"] == "PASS" for test in checks_results.values())

    date_range = (
        params["date_range_start"]
        if params["date_range_start"] == params["date_range_end"]
        else f'{params["date_range_start"]} a {params["date_range_end"]}'
    )

    formatted_messages = [
        ":green_circle: " if test_check else ":red_circle: ",
        f"**[DEV]Data Quality Checks - {context.get('flow_name')} - {date_range}**\n\n",
    ]

    for table_id, tests in checks_list.items():
        formatted_messages.append(
            f"*{table_id}:*\n"
            + "\n".join(
                f'{":white_check_mark:" if checks_results[test_id]["result"] == "PASS" else ":x:"} '
                f'{test["description"]}'
                for test_id, test in tests.items()
            )
        )

    formatted_messages.append("\n\n")
    formatted_messages.append(
        ":tada: **Status:** Sucesso"
        if test_check
        else ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n"
    )

    formatted_messages.append(dados_tag)
    format_send_discord_message(formatted_messages, webhook_url)
