# -*- coding: utf-8 -*-
"""Module with the base class for data extractions"""
from abc import ABC, abstractmethod
from typing import Union

from pipelines.utils.fs import save_local_file


class DataExtractor(ABC):
    """
    Abstract class for all Data Extractors
    For single page extractions:
        - implement the method "_get_data"
    For multi page extractions:
        - implement the method "_get_data" and overwrite the methods
          "_prepare_next_page" and "_check_if_last_page" according to the extraction logic


    Args:
        filetype (Union[None, str]): The extracted data file type (json, csv...)

    Attributes:
        last_page (bool): True if is the last page to capture
        page_num (int): Current page number
        data (list): The data extracted from all pages
        page_data (Union[None, dict, list[dict], str]): The data from the current page
        error (Union[None, str]): The error traceback
        save_path (str): Local path to save the extracted data
    """

    def __init__(self, save_path: str) -> None:
        self.save_path = save_path
        self.data = []
        self.last_page = False
        self.page_data = None
        self.current_page = 0

    @abstractmethod
    def _get_data(self) -> Union[list[dict], dict, str]:
        """Abstract method to get the data from one page

        Returns:
            Union[list[dict], dict, str]: The extracted data
        """

    def _prepare_next_page(self):
        """Prepare the object to get the next page"""
        if isinstance(self.page_data, list):
            self.data += self.page_data
        else:
            self.data.append(self.page_data)

        self.last_page = self._check_if_last_page()

        self.current_page += 1

    def _check_if_last_page(self) -> bool:
        """
        Method to check if it is the last page.
        Overwrite it if you want to implement a paginated extractor
        """
        return True

    def extract(self) -> Union[dict, list[dict], str]:
        """
        Get data from all pages using the methods implemented by concrete classes
        """
        while not self.last_page:
            self.page_data = self._get_data()
            self._prepare_next_page()

        return self.data

    def save_raw_local(self):
        """
        Saves the data at the local file path
        """
        save_local_file(
            filepath=self.save_path,
            data=self.data,
        )
