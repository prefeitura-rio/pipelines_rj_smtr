# -*- coding: utf-8 -*-
"""Tasks dos flows de controle"""
import re
from typing import Optional

from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants
from pipelines.utils.discord import format_send_discord_message
from pipelines.utils.secret import get_secret


@task
def set_redis_keys(keys: dict):
    client = get_redis_client()
    for key, value in keys.items():
        log(
            f"""
            key = {key}

            valor atual = {client.get(key)}

            novo valor = {value}
        """
        )
        client.set(key, value)


@task(nout=2)
def parse_source_freshness_output(dbt_output: str) -> tuple[bool, Optional[list[str]]]:
    """
    Analisa a saída do comando `dbt source freshness` para identificar fontes desatualizadas

    Args:
        dbt_output (str): Texto completo da saída do comando `dbt source freshness`

    Returns:
        tuple[bool, Optional[list[str]]]:
            - Um booleano que é True se existirem fontes com alerta (WARN), False caso contrário
            - Uma lista com os nomes das fontes desatualizadas ou uma lista vazia
    """
    failed_sources = re.findall(r"WARN freshness of ([\w\.]+)", dbt_output)

    return len(failed_sources) > 0, failed_sources


@task
def source_freshness_notify_discord(failed_sources: list[str]):
    """
    Envia uma notificação para o Discord alertando sobre fontes de dados desatualizadas

    Args:
        failed_sources (list[str]): Lista com os nomes das fontes desatualizadas
    """
    webhook_url = get_secret(secret_path=constants.WEBHOOKS_SECRET_PATH.value)["dataplex"]
    metions_tag = f" - <@&{constants.OWNERS_DISCORD_MENTIONS.value['dados_smtr']['user_id']}>\n\n"
    formatted_messages = [f":red_circle: **Sources desatualizados** {metions_tag}"]

    for source in failed_sources:
        formatted_messages.append(f":warning: {source}\n")

    format_send_discord_message(formatted_messages=formatted_messages, webhook_url=webhook_url)
