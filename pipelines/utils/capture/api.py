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
    Class for get raw data from API's

    Args:
        url (str): Endpoint URL
        headers (Union[None, dict]): Request headers
        params (Union[None, dict]): Request url params
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
    Class for get raw data from API's, using top/skip pagination standard

    Args:
        url (str): Endpoint URL
        headers (Union[None, dict]): Request headers
        params (Union[None, dict]): Request url params
        top_param_name (str): Parameter that represents the "top"
        skip_param_name (str): Parameter that represents the "skip"
        page_size (int): Maximum page size
    """

    def __init__(
        self,
        url: str,
        headers: Union[dict, None],
        params: dict,
        top_param_name: str,
        skip_param_name: str,
        page_size: int,
        max_pages: int,
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
        self.max_pages = max_pages

    def _prepare_next_page(self):
        super()._prepare_next_page()
        self.params[self.skip_param_name] += self.page_size

    def _check_if_last_page(self) -> bool:
        return len(self.page_data) < self.page_data or self.max_pages == self.current_page + 1
