# -*- coding: utf-8 -*-
"""Module to get data from databases"""
import pandas as pd
from sqlalchemy import create_engine

from pipelines.utils.capture.base import DataExtractor
from pipelines.utils.fs import get_filetype


class DBExtractor(DataExtractor):
    """
    Class for get raw data from Databases

    Args:
        query (str): The SQL Query to execute
        engine (str): The database management system
        host (str): The database host
        user (str): The database user
        password (str): User's password
        database (str): The database to connect
    """

    def __init__(
        self,
        query: str,
        engine: str,
        host: str,
        user: str,
        password: str,
        database: str,
        save_path: str,
    ) -> None:
        super().__init__(save_path=save_path)
        if get_filetype(save_path) != "json":
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
        max_retries = 10
        for retry in range(1, max_retries + 1):
            try:
                print(f"[ATTEMPT {retry}/{max_retries}]: {self.query}")
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
    Class for get raw data from Databases using top/limit pagination

    Args:
        query (str): the SQL Query to execute
        engine (str): The database management system
        host (str): The database host
        user (str): The database user
        password (str): User's password
        database (str): The database to connect
        page_size (int): The maximum number of rows returned by the paginated query
        max_pages (int): The maximum number of paginated queries to execute
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
        max_pages: int,
        save_path: str,
    ) -> None:
        super().__init__(
            query=query,
            engine=engine,
            host=host,
            user=user,
            password=password,
            database=database,
            save_path=save_path,
        )
        self.offset = 0
        self.base_query = f"{query} LIMIT {page_size}"
        self.query = f"{self.base_query} OFFSET 0"
        self.max_pages = max_pages
        self.page_size = page_size

    def _prepare_next_page(self):
        super()._prepare_next_page()
        self.offset += self.page_size
        self.query = f"{self.base_query} OFFSET {self.offset}"

    def _check_if_last_page(self):
        return len(self.page_data) < self.page_data or self.max_pages == self.current_page + 1
