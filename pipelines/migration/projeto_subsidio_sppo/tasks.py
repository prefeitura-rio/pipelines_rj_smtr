# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from datetime import datetime, timedelta

from prefect import task
from prefect.tasks.core.operators import GreaterThanOrEqual

from pipelines.constants import constants as smtr_constants
from pipelines.migration.projeto_subsidio_sppo.constants import constants
from pipelines.migration.tasks import perform_checks_for_table  # perform_check,
from pipelines.utils.discord import format_send_discord_message
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import log


@task
def check_param(param: str) -> bool:
    """
    Check if param is None
    """
    return param is None


@task
def subsidio_data_quality_check(
    mode: str, params: dict, code_owners: list = None, check_params: dict = None
) -> bool:
    """
    Verifica qualidade de dados para o processo de apuração de subsídio

    Args:
        mode (str): Modo de execução (pre ou pos)
        params (dict): Parameters for the checks
        # code_owners (list): Code owners to be notified
        check_params (dict): queries and order columns for the checks

    Returns:
        test_check (bool): True if all checks passed, False otherwise
    """

    if mode not in ["pre", "pos"]:
        raise ValueError("Mode must be 'pre' or 'pos'")

    if check_params is None:
        check_params = constants.SUBSIDIO_SPPO_DATA_CHECKS_PARAMS.value

    if code_owners is None:
        code_owners = constants.SUBSIDIO_SPPO_CODE_OWNERS.value

    checks = dict()

    request_params = {
        "start_timestamp": f"""{params["start_date"]} 00:00:00""",
        "end_timestamp": (
            datetime.strptime(params["end_date"], "%Y-%m-%d") + timedelta(hours=27)
        ).strftime("%Y-%m-%d %H:%M:%S"),
    }

    gte = GreaterThanOrEqual()
    gte_result = gte.run(params["start_date"], constants.DATA_SUBSIDIO_V9_INICIO.value)

    if mode == "pos":
        request_params["end_timestamp"] = f"""{params["end_date"]} 00:00:00"""
        request_params["dataset_id"] = constants.SUBSIDIO_SPPO_DASHBOARD_DATASET_ID.value
        if gte_result:
            request_params["dataset_id_v2"] = constants.SUBSIDIO_SPPO_DASHBOARD_V2_DATASET_ID.value
            request_params[
                "table_id_v2"
            ] = constants.SUBSIDIO_SPPO_DASHBOARD_SUMARIO_TABLE_ID_V2.value
        else:
            request_params["dataset_id_v2"] = constants.SUBSIDIO_SPPO_DASHBOARD_DATASET_ID.value
            request_params["table_id_v2"] = constants.SUBSIDIO_SPPO_DASHBOARD_SUMARIO_TABLE_ID.value

    checks_list = (
        constants.SUBSIDIO_SPPO_DATA_CHECKS_PRE_LIST.value
        if mode == "pre"
        else constants.SUBSIDIO_SPPO_DATA_CHECKS_POS_LIST.value
    )

    for (
        table_id,
        test_check_list,
    ) in checks_list.items():
        checks[table_id] = perform_checks_for_table(
            table_id, request_params, test_check_list, check_params
        )

    log(checks)

    date_range = (
        params["start_date"]
        if params["start_date"] == params["end_date"]
        else f'{params["start_date"]} a {params["end_date"]}'
    )

    webhook_url = get_secret(secret_path=constants.SUBSIDIO_SPPO_SECRET_PATH.value)[
        "discord_data_check_webhook"
    ]

    test_check = all(table["status"] for sublist in checks.values() for table in sublist)

    formatted_messages = [
        ":green_circle: " if test_check else ":red_circle: ",
        f"**{mode.capitalize()}-Data Quality Checks - Apuração de Subsídio - {date_range}**\n\n",
    ]

    if "general" in checks:
        formatted_messages.extend(
            f'{":white_check_mark:" if check["status"] else ":x:"} {check["desc"]}\n'
            for check in checks["general"]
        )

    format_send_discord_message(formatted_messages, webhook_url)

    for table_id, checks_ in checks.items():
        if table_id != "general":
            formatted_messages = [
                f"*{table_id}:*\n"
                + "\n".join(
                    f'{":white_check_mark:" if check["status"] else ":x:"} {check["desc"]}'
                    for check in checks_
                )
            ]
            format_send_discord_message(formatted_messages, webhook_url)

    formatted_messages = ["\n\n"]

    if mode == "pre":
        formatted_messages.append(
            ""
            if test_check
            else """:warning: **Status:** Necessidade de revisão dos dados de entrada!\n"""
        )

    if mode == "pos":
        formatted_messages.append(
            ":tada: **Status:** Sucesso"
            if test_check
            else ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n"
        )

    # fmt: off
    if not test_check:
        at_code_owners = [
            f'   - <@{smtr_constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            if smtr_constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["type"] == "user"
            else f'   - <@!{smtr_constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            if smtr_constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["type"] == "user_nickname"
            else f'   - <#{smtr_constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            if smtr_constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["type"] == "channel"
            else f'   - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS.value[code_owner]["user_id"]}>\n'
            for code_owner in code_owners
        ]

        formatted_messages.extend(at_code_owners)
    # fmt: on
    format_send_discord_message(formatted_messages, webhook_url)

    return test_check
