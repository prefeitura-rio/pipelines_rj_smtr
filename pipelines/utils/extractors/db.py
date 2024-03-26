# -*- coding: utf-8 -*-
"""Module to get data from databases"""
import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from sqlalchemy import create_engine

from pipelines.utils.extractors.base import DataExtractor
from pipelines.utils.fs import get_filetype


class DBExtractor(DataExtractor):
    """
    Classe para extrair dados de banco de dados

    Args:
        query (str): o SELECT para ser executada
        engine (str): O banco de dados (postgres ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)
        save_filepath (str): Caminho para salvar os dados
    """

    def __init__(
        self,
        query: str,
        engine: str,
        host: str,
        user: str,
        password: str,
        database: str,
        save_filepath: str,
    ) -> None:
        super().__init__(save_filepath=save_filepath)
        if get_filetype(save_filepath) != "json":
            raise ValueError("File type must be json")

        self.query = query
        engine_mapping = {
            "mysql": {"driver": "pymysql", "port": "3306"},
            "postgresql": {"driver": "psycopg2", "port": "5432"},
        }
        engine_details = engine_mapping[engine]
        driver = engine_details["driver"]
        port = engine_details["port"]
        connection_string = f"{engine}+{driver}://{user}:{password}@{host}:{port}/{database}"
        self.connection = create_engine(connection_string)

    def _get_data(self) -> list[dict]:
        """
        Executa a query e retorna os dados como JSON

        Returns:
            list[dict]: Os dados retornados pela query
        """
        max_retries = 10
        for retry in range(1, max_retries + 1):
            try:
                log(f"[ATTEMPT {retry}/{max_retries}]: {self.query}")
                data = pd.read_sql(sql=self.query, con=self.connection).to_dict(orient="records")
                for d in data:
                    for k, v in d.items():
                        if pd.isna(v):
                            d[k] = None
                break
            except Exception as err:
                if retry == max_retries:
                    raise err

        return data


class PaginatedDBExtractor(DBExtractor):
    """
    Classe para extrair dados de um banco de dados com paginação offset/limit

    Args:
        query (str): o SELECT para ser executada (sem o limit e offset)
        engine (str): O banco de dados (postgres ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)
        page_size (int): Número de linhas por página
        max_pages (int): Número máximo de páginas para serem extraídas
        save_filepath (str): Caminho para salvar os dados
    """

    def __init__(
        self,
        query: str,
        engine: str,
        host: str,
        user: str,
        password: str,
        database: str,
        page_size: int,
        save_filepath: str,
    ) -> None:
        super().__init__(
            query=query,
            engine=engine,
            host=host,
            user=user,
            password=password,
            database=database,
            save_filepath=save_filepath,
        )
        self.offset = 0
        self.base_query = f"{query} LIMIT {page_size}"
        self.query = f"{self.base_query} OFFSET 0"
        self.page_size = page_size

    def _prepare_next_page(self):
        """
        Incrementa o offset e concatena na query
        """
        super()._prepare_next_page()
        self.offset += self.page_size
        self.query = f"{self.base_query} OFFSET {self.offset}"

    def _check_if_last_page(self):
        """
        Verifica se o número de dados retornados na última página é menor que o máximo
        ou se chegou ao limite de numero de páginas
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
