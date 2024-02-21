# -*- coding: utf-8 -*-
"""Module with the base class for data extractions"""
from abc import ABC, abstractmethod
from typing import Union

from pipelines.utils.fs import save_local_file


class DataExtractor(ABC):
    """
    Classe abstrata para criar Data Extractors

    Para criar extrações com uma página:
        - Implemente o método "_get_data"

    Para criar extrações com várias páginas:
        - Implemente o método "_get_data"
        - Sobrescreva os métodos "_prepare_next_page" and "_check_if_last_page"
            de acordo com a lógica de paginação da sua extração

    Args:
        save_filepath (str): O caminho para salvar os dados extraídos

    Attributes:
        save_filepath (str): O caminho para salvar os dados extraídos
        data (list): Os dados extraídos de todas as páginas
        last_page (bool): Se é a última página da captura ou não
        page_data: Os dados extraídos da página atual
        current_page (int): o número da página atual, iniciando em 0
    """

    def __init__(self, save_filepath: str) -> None:
        self.save_filepath = save_filepath
        self.data = []
        self.last_page = False
        self.page_data = None
        self.current_page = 0

    @abstractmethod
    def _get_data(self) -> Union[list[dict], dict, str]:
        """
        Método abstrato para extrair dos dados de uma página

        Para implementar, crie a lógica da extração, retornando
        uma lista de dicionários, um dicionário ou uma string

        Returns:
            Union[list[dict], dict, str]: Os dados extraídos
        """

    def _prepare_next_page(self):
        """
        Prepara o objeto para extrair a próxima página

        Coloca os dados da página na lista de dados gerais
        Verifica se é a última página
        Incrementa o atributo current_page em 1
        """
        if isinstance(self.page_data, list):
            self.data += self.page_data
        else:
            self.data.append(self.page_data)

        self.last_page = self._check_if_last_page()

        self.current_page += 1

    def _check_if_last_page(self) -> bool:
        """
        Verifica se é a última página
        Para implementar uma extração paginada,
        sobrescreva esse método com a lógica de verificação
        """
        return True

    def extract(self) -> Union[dict, list[dict], str]:
        """
        Extrai os dados completos de todas as páginas

        Returns:
            Union[dict, list[dict], str]: Os dados retornados
        """
        while not self.last_page:
            self.page_data = self._get_data()
            self._prepare_next_page()

        return self.data

    def save_raw_local(self):
        """
        Salva os dados extraídos localmente
        """
        save_local_file(
            filepath=self.save_filepath,
            data=self.data,
        )
