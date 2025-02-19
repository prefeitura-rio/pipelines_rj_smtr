# -*- coding: utf-8 -*-
"""Tasks dos flows de controle"""
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client


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
