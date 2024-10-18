# -*- coding: utf-8 -*-
"""Module to get data from apis"""
import time
from typing import Union

import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants


def get_raw_api(
    url: str,
    headers: Union[None, dict] = None,
    params: Union[None, dict] = None,
    raw_filetype: str = "json",
) -> Union[str, dict, list[dict]]:
    """
    Função para extrair dados de API com uma página

    Args:
        url (str): URL para fazer o request
        headers (Union[None, dict]): Headers para o request
        params (Union[None, dict]): Paramêtros para o request
        raw_filetype (str): Tipo de dado do arquivo (csv, json, ...)

    Returns:
        Union[str, dict, list[dict]]: Dados capturados da API
    """
    for retry in range(constants.MAX_RETRIES.value):
        response = requests.get(
            url,
            headers=headers,
            timeout=constants.MAX_TIMEOUT_SECONDS.value,
            params=params,
        )

        if response.ok:
            break
        if response.status_code >= 500:
            log(f"Server error {response.status_code}")
            if retry == constants.MAX_RETRIES.value - 1:
                response.raise_for_status()
            time.sleep(60)
        else:
            response.raise_for_status()

    if raw_filetype == "json":
        data = response.json()
    else:
        data = response.text

    return data


def get_raw_api_top_skip(
    url: str,
    headers: Union[None, dict],
    params: Union[None, dict],
    top_param_name: str,
    skip_param_name: str,
    page_size: int,
    max_page: int,
) -> list[dict]:
    """
    Função para extrair dados de API paginada do tipo top e skip. Deve
    Args:
        url (str): URL para fazer o request
        headers (Union[None, dict]): Headers para o request
        params (Union[None, dict]): Paramêtros para o request
        top_param_name (str): Nome do parâmetro que define a quantidade de registros em uma página
        skip_param_name (str): Nome do parâmetro que define quantos registros pular
        page_size (int): Número máximo de registros em uma página
        max_page (int): Número máximo de páginas a serem capturadas

    Returns:
        list[dict]: Dados capturados da API
    """
    data = []
    params[top_param_name] = page_size
    params[skip_param_name] = 0

    for page in range(max_page):
        page_data = get_raw_api(url=url, headers=headers, params=params, raw_filetype="json")
        page_data_len = len(page_data)
        log(
            f"""
            Page size: {page_size}
            Current page: {page}
            Current page returned {page_data_len} rows"""
        )
        data += page_data
        if page_data_len < page_size:
            log("Last page, ending extraction")
            break

        params[skip_param_name] += page_size
    return data
