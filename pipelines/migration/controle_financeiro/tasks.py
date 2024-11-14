# -*- coding: utf-8 -*-
"""
tasks for rj-smtr.controle_financeiro
"""

from datetime import date, timedelta
from typing import Union

import pandas as pd
import requests
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants as smtr_constants
from pipelines.migration.controle_financeiro.constants import constants
from pipelines.migration.utils import save_raw_local_func
from pipelines.utils.secret import get_secret


@task
def get_cct_arquivo_retorno_redis_key(mode: str) -> str:
    """
    Gets the key to search and store pending dates on Redis

    Args:
        mode (str): dev or prod

    Returns:
        str: Redis key
    """
    return (
        mode
        + "."
        + smtr_constants.CONTROLE_FINANCEIRO_DATASET_ID.value
        + "."
        + constants.ARQUIVO_RETORNO_TABLE_ID.value
    )


@task(nout=2)
def create_cct_arquivo_retorno_params(
    redis_key: str, start_date: Union[str, None], end_date: Union[str, None]
) -> tuple[dict, dict]:
    """
    Create parameters to get data from cct api's arquivoPublicacao

    Args:
        redis_key (str): Redis key to get pending dates
        start_date (str): Initial data_ordem to filter
        end_date (str): Final data_ordem to filter

    Returns:
        dict: headers
        dict: parameters
    """
    auth_resp = requests.post(
        f"{constants.CCT_API_BASE_URL.value}/auth/admin/email/login",
        data=get_secret(constants.CCT_API_SECRET_PATH.value),
        timeout=smtr_constants.MAX_TIMEOUT_SECONDS.value,
    )
    auth_resp.raise_for_status()
    headers = {"Authorization": f"Bearer {auth_resp.json()['token']}"}

    if start_date is not None and end_date is not None:
        return headers, {
            "dt_inicio": start_date,
            "dt_fim": end_date,
        }

    redis_client = get_redis_client()

    log(f"Getting pending dates on Redis. key = {redis_key}")
    redis_return = redis_client.get(redis_key)
    log(f"Got value from Redis: {redis_return}")

    if redis_return is None:
        params = {
            "dt_inicio": "2024-05-09",
            "dt_fim": date.today().isoformat(),
        }

    else:
        params = {
            "dt_inicio": (
                date.fromisoformat(redis_return["last_date"]) + timedelta(days=1)
            ).isoformat(),
            "dt_fim": date.today().isoformat(),
        }

    return headers, params


@task
def get_raw_cct_arquivo_retorno(headers: dict, params: dict, local_filepath: str) -> str:
    """
    Get data from cct api arquivoPublicacao

    Args:
        headers (dict): Request headers
        params (list[dict]): List of request query params
        local_filepath (str): Path to save the data

    Returns:
        str: filepath to raw data
    """
    url = f"{constants.CCT_API_BASE_URL.value}/cnab/arquivoPublicacao"
    log(
        f"""Getting raw data:
        url: {url},
        params: {params}"""
    )
    resp = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=smtr_constants.MAX_TIMEOUT_SECONDS.value,
    )

    resp.raise_for_status()
    data = resp.json()

    log(f"returned {len(data)} rows")

    return save_raw_local_func(data=data, filepath=local_filepath)


@task
def cct_arquivo_retorno_save_redis(redis_key: str, raw_filepath: str):
    """
    Set control info on Redis

    Args:
        redis_key (str): Key on Redis
        raw_filepath (str): Filepath to raw data
    """
    df = pd.read_json(raw_filepath)
    if df.empty:  # pylint: disable=E1101
        return
    df["dataVencimento"] = pd.to_datetime(df["dataVencimento"]).dt.strftime("%Y-%m-%d")
    all_returned_dates = df["dataVencimento"].unique().tolist()

    log(f"The API returned the following dates: {sorted(all_returned_dates)}")

    redis_client = get_redis_client()
    redis_return = redis_client.get(redis_key)

    if redis_return is None:
        redis_return = {}

    redis_return["last_date"] = max(
        [df["dataVencimento"].max(), redis_return.get("last_date", "2024-05-09")]
    )

    log(
        f"""
        Saving values on redis
        last_date: {redis_return["last_date"]}
        """
    )

    redis_client.set(redis_key, redis_return)
