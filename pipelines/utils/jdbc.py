# -*- coding: utf-8 -*-
from typing import List, Tuple

import jaydebeapi as jdb

from pipelines.utils.secret import get_secret
from pipelines.utils.utils import log


class JDBC:
    def __init__(self, db_params_secret_path: str, environment: str = "staging") -> None:
        self._environment = environment
        self._secret_path = db_params_secret_path
        self._conn_kwargs = self.get_conn_kwargs()
        self._connection = self.connect()
        self._cursor = self.get_cursor()

    def get_conn_kwargs(self):

        data = get_secret(secret_path=self._secret_path, environment=self._environment)
        conn_kwargs = dict(
            jclassname=data["jclassname"],
            user=data["user"],
            password=data["password"],
            url=data["url"],
            jars=[data["jars"]],
        )
        return conn_kwargs

    def connect(self):
        data = get_secret(secret_path=self._secret_path, environment=self._environment)

        return jdb.connect(
            jclassname=data["jclassname"],
            url=data["url"],
            jars=rf"{data['jars']}",
            driver_args=[data["user"], data["password"]],
        )

    def test_connection(self) -> Tuple[bool, str | None]:
        """
        Tests the connection to the SERPRO database.

        Returns:
            tuple: (success, error_message)
                - success (bool): True if the connection was successful, False otherwise
                - error_message (str | None): Error message in case of failure, None if successful
        """
        try:
            log("Testando conexão atual com o banco de dados SERPRO")
            self._cursor.execute("SELECT 1")
            self._cursor.fetchone()
            log("Conexão atual com SERPRO está operacional")
            return True, None
        except Exception as e:
            log(f"Conexão atual com SERPRO não está operacional: {str(e)}", level="warning")
            return False, str(e)

    def get_cursor(self):
        """
        Returns a cursor for the JDBC database.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the JDBC database.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the JDBC database.
        """
        return [column[0] for column in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the JDBC database.
        """
        return [list(item) for item in self._cursor.fetchmany(batch_size)]

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the JDBC database.
        """
        return [list(item) for item in self._cursor.fetchall()]

    def close(self) -> None:
        """
        Closes the cursor and the JDBC connection.
        """
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
