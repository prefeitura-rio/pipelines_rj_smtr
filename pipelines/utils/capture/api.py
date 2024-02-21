# -*- coding: utf-8 -*-
"""Module to get data from apis"""
import time
from typing import Union

import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.capture.base import DataExtractor
from pipelines.utils.fs import get_filetype


class APIExtractor(DataExtractor):
    """
    Classe para extrair dados de API com uma página

    Args:
        url (str): URL para fazer o request
        headers (Union[None, dict]): Headers para o request
        params (Union[None, dict]): Paramêtros para o request
        save_filepath (str): Caminho para salvar os dados
    """

    def __init__(
        self,
        url: str,
        headers: Union[None, dict],
        params: Union[None, dict],
        save_filepath: str,
    ) -> None:
        super().__init__(save_filepath=save_filepath)
        self.url = url
        self.params = params
        self.headers = headers
        self.filetype = get_filetype(save_filepath)

    def _get_data(self) -> Union[list[dict], dict, str]:
        """
        Extrai os dados da API

        Returns:
            Union[list[dict], dict, str]: list[dict] ou dict para APIs json
                str para outros tipos
        """
        for retry in range(constants.MAX_RETRIES.value):
            response = requests.get(
                self.url,
                headers=self.headers,
                timeout=constants.MAX_TIMEOUT_SECONDS.value,
                params=self.params,
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

        if self.filetype == "json":
            data = response.json()
        else:
            data = response.text

        return data


class APIExtractorTopSkip(APIExtractor):
    """
    Classe para extrair dados de uma API paginada do tipo Top/Skip

    Args:
        url (str): URL para fazer o request
        headers (Union[None, dict]): Headers para o request
        params (Union[None, dict]): Paramêtros para o request (exceto os de top e skip)
        top_param_name (str): Nome do parâmetro de top (que define o tamanho da página)
        skip_param_name (str): Nome do parâmetro de skip (quantidade de linhas a serem puladas)
        page_size (int): Número de registros por página (valor a ser passado no parâmetro de top)
        save_filepath (str): Caminho para salvar os dados
    """

    def __init__(
        self,
        url: str,
        headers: Union[dict, None],
        params: dict,
        top_param_name: str,
        skip_param_name: str,
        page_size: int,
        save_filepath: str,
    ) -> None:
        super().__init__(
            url=url,
            headers=headers,
            params=params,
            save_filepath=save_filepath,
        )

        if self.filetype != "json":
            raise ValueError("File Type must be json")

        self.params[top_param_name] = page_size
        self.skip_param_name = skip_param_name
        self.params[skip_param_name] = 0
        self.page_size = page_size

    def _prepare_next_page(self):
        """
        Incrementa o valor do skip para buscar a próxima página
        """
        super()._prepare_next_page()
        self.params[self.skip_param_name] += self.page_size

    def _check_if_last_page(self) -> bool:
        """
        Verifica se a página tem menos registros do que o máximo
        ou se chegou ao limite de páginas
        """
        page_data_len = len(self.page_data)
        current_page = self.current_page + 1
        log(
            f"""
            Page size: {self.page_size}
            Current page: {current_page}
            Current page returned {page_data_len} rows"""
        )

        last_page = page_data_len < self.page_size
        if last_page:
            log("Last page, ending extraction")
        return last_page
