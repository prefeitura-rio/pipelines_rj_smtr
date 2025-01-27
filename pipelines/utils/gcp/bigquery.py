# -*- coding: utf-8 -*-
"""Módulo para interagir com tabelas e datasets do BigQuery"""

import basedosdados as bd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.gcp.base import GCPBase


class Dataset(GCPBase):
    """
    Classe que representa um Dataset do BigQuery

    Args:
        dataset_id (str): dataset_id no BigQuery
        env (str): prod ou dev
        location (str): local dos dados do dataset
    """

    def __init__(self, dataset_id: str, env: str, location: str = "US") -> None:
        super().__init__(
            dataset_id=dataset_id,
            table_id="",
            bucket_names=None,
            env=env,
        )
        self.location = location

    def exists(self) -> bool:
        """
        Se o Dataset existe no BigQuery

        Returns
            bool: True se já existir no BigQuery, caso contrário, False
        """
        try:
            self.client("bigquery").get_dataset(self.dataset_id)
            return True
        except NotFound:
            return False

    def create(self):
        """
        Cria o Dataset do BigQuery
        """
        if not self.exists():
            dataset_full_name = f"{constants.PROJECT_NAME.value[self.env]}.{self.dataset_id}"
            dataset_obj = bigquery.Dataset(dataset_full_name)
            dataset_obj.location = self.location
            log(f"Creating dataset {dataset_full_name} | location: {self.location}")
            self.client("bigquery").create_dataset(dataset_obj)
            log("Dataset created!")
        else:
            log("Dataset already exists")


class BQTable(GCPBase):
    """
    Classe para manipular tabelas do BigQuery

    Args:
        env (str): prod ou dev
        dataset_id (str): dataset_id no BigQuery
        table_id (str): table_id no BigQuery
        bucket_names (dict): nome dos buckets de prod e dev associados ao objeto
    """

    def __init__(  # pylint: disable=R0913
        self,
        env: str,
        dataset_id: str,
        table_id: str,
        bucket_names: dict = None,
    ) -> None:
        self.table_full_name = None
        super().__init__(
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_names=bucket_names,
            env=env,
        )

    def set_env(self, env: str):
        """
        Altera o ambiente

        Args:
            env (str): prod ou dev

        Returns:
            self
        """
        super().set_env(env=env)

        self.table_full_name = (
            f"{constants.PROJECT_NAME.value[env]}.{self.dataset_id}.{self.table_id}"
        )
        return self

    def exists(self) -> bool:
        """
        Checagem se a tabela existe no BigQuery

        Returns:
            bool: Se existe ou não
        """
        try:
            return bool(self.client("bigquery").get_table(self.table_full_name))
        except NotFound:
            return False

    def get_table_min_max_value(self, field_name: str, kind: str):
        """
        Busca o valor máximo ou mínimo de um campo na tabela

        Args:
            field_name (str): Nome do campo
            kind (str): max ou min

        Returns:
            Any: Valor do campo
        """
        log(f"Getting {kind} value for {self.table_id}")
        query = f"""
        SELECT
            {kind}({field_name})
        FROM {self.table_full_name}
        """
        result = bd.read_sql(query=query)

        return result.iloc[0][0]
