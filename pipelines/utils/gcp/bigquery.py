# -*- coding: utf-8 -*-
"""Módulo para interagir com tabelas e datasets do BigQuery"""
import csv
from datetime import datetime, timedelta
from typing import Callable, Optional

import pandas as pd
import pandas_gbq
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.gcp.base import GCPBase
from pipelines.utils.gcp.storage import Storage
from pipelines.utils.utils import convert_timezone, cron_date_range, cron_get_last_date


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
        result = pandas_gbq.read_gbq(query, project_id=constants.PROJECT_NAME.value[self.env])

        return result.iloc[0][0]


class SourceTable(BQTable):
    """
    Classe para manipular dados capturados pelos flows

    Args:
        source_name (str): Nome da fonte de dados
        table_id (str): table_id no BigQuery
        first_timestamp (datetime): Primeira timestamp com dados
        schedule_cron (str): Expressão cron contendo a frequência de atualização dos dados
        primary_keys (list[str]): Lista com o nome das primary keys da tabela
        pretreatment_reader_args (dict): Argumentos para leitura dos dados. São utilizados
            nas funções pd.read_csv ou pd.read_json dependendo do tipo de dado
        pretreat_funcs (Callable[[pd.DataFrame, datetime, list[str]], pd.DataFrame]): funções para
            serem executadas antes de transformar em nested. Devem receber os argumentos:
                data (pd.DataFrame)
                timestamp (datetime)
                primary_keys (list[str])
            e retornar um pd.DataFrame
        bucket_names (dict): nome dos buckets de prod e dev associados ao objeto
        partition_date_only (bool): caso positivo, irá criar apenas a partição de data. Caso
            negativo, cria partição de data e de hora
        max_recaptures (int): número máximo de recapturas executadas de uma só vez
        raw_filetype (str): tipo do dado (json, csv, txt)

    """

    def __init__(  # pylint: disable=R0913
        self,
        source_name: str,
        table_id: str,
        first_timestamp: datetime,
        schedule_cron: str,
        primary_keys: list[str] = None,
        pretreatment_reader_args: dict = None,
        pretreat_funcs: Callable[[pd.DataFrame, datetime, list[str]], pd.DataFrame] = None,
        bucket_names: dict = None,
        partition_date_only: bool = False,
        max_recaptures: int = 60,
        raw_filetype: str = "json",
        file_chunk_size: Optional[int] = None,
    ) -> None:
        self.source_name = source_name
        super().__init__(
            dataset_id="source_" + source_name,
            table_id=table_id,
            env="dev",
            bucket_names=bucket_names,
        )
        self.bucket_name = bucket_names
        self.raw_filetype = raw_filetype
        self.primary_keys = primary_keys
        self.partition_date_only = partition_date_only
        self.max_recaptures = max_recaptures
        self.first_timestamp = convert_timezone(first_timestamp)
        self.pretreatment_reader_args = pretreatment_reader_args
        self.pretreat_funcs = pretreat_funcs or []
        self.schedule_cron = schedule_cron
        self.file_chunk_size = file_chunk_size

    def _create_table_schema(self, sample_filepath: str) -> list[bigquery.SchemaField]:
        """
        Cria schema para os argumentos da criação de tabela externa no BQ
        """
        log("Creating table schema...")
        columns = next(csv.reader(open(sample_filepath, encoding="utf-8")))

        log(f"Columns: {columns}")
        schema = [
            bigquery.SchemaField(name=col, field_type="STRING", description=None) for col in columns
        ]
        log("Schema created!")
        return schema

    def _create_table_config(self, sample_filepath: str) -> bigquery.ExternalConfig:
        """
        Cria as configurações da tabela externa no BQ
        """
        external_config = bigquery.ExternalConfig("CSV")
        external_config.options.skip_leading_rows = 1
        external_config.options.allow_quoted_newlines = True
        external_config.autodetect = False
        external_config.schema = self._create_table_schema(sample_filepath=sample_filepath)
        external_config.options.field_delimiter = ","
        external_config.options.allow_jagged_rows = False

        uri = f"gs://{self.bucket_name}/source/{self.dataset_id}/{self.table_id}/*"
        external_config.source_uris = uri
        hive_partitioning = HivePartitioningOptions()
        hive_partitioning.mode = "AUTO"
        hive_partitioning.source_uri_prefix = uri.replace("*", "")
        external_config.hive_partitioning = hive_partitioning

        return external_config

    def get_last_scheduled_timestamp(self, timestamp: datetime) -> datetime:
        """
        Retorna o último timestamp programado antes do timestamp fornecido
        com base na expressão cron configurada

        Args:
            timestamp (datetime): O timestamp de referência

        Returns:
            datetime: O último timestamp programado com base na expressão cron
        """
        return cron_get_last_date(cron_expr=self.schedule_cron, timestamp=timestamp)

    def get_uncaptured_timestamps(self, timestamp: datetime, retroactive_days: int = 2) -> list:
        """
        Retorna todas as timestamps não capturadas até um datetime

        Args:
            timestamp (datetime): filtro limite para a busca
            retroactive_days (int): número de dias anteriores à timestamp informada que a busca
                irá verificar

        Returns:
            list: Lista com as timestamps não capturadas
        """
        st = Storage(
            env=self.env,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            bucket_names=self.bucket_names,
        )
        initial_timestamp = max(timestamp - timedelta(days=retroactive_days), self.first_timestamp)
        full_range = cron_date_range(
            cron_expr=self.schedule_cron, start_time=initial_timestamp, end_time=timestamp
        )
        days_to_check = pd.date_range(initial_timestamp.date(), timestamp.date())

        files = []
        for day in days_to_check:
            prefix = f"source/{self.dataset_id}/{self.table_id}/data={day.date().isoformat()}/"
            files = files + [
                convert_timezone(datetime.strptime(b.name.split("/")[-1], "%Y-%m-%d-%H-%M-%S.csv"))
                for b in st.bucket.list_blobs(prefix=prefix)
                if ".csv" in b.name and len(b.name.split("/")[-1]) == 23
            ]

        return [d for d in full_range if d not in files][: self.max_recaptures]

    def upload_raw_file(self, raw_filepath: str, partition: str):

        st_obj = Storage(
            env=self.env,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            bucket_names=self.bucket_names,
        )

        st_obj.upload_file(
            mode="raw",
            filepath=raw_filepath,
            partition=partition,
        )

    def create(self, sample_filepath: str, location: str = "US"):
        """
        Cria tabela externa do BQ

        Args:
            location (str): Localização do dataset
            sample_filepath (str): Caminho com dados da tabela (usados para criar o schema)
        """
        log(f"Creating External Table: {self.table_full_name}")
        dataset_obj = Dataset(dataset_id=self.dataset_id, env=self.env, location=location)
        dataset_obj.create()

        client = self.client("bigquery")

        bq_table = bigquery.Table(self.table_full_name)
        bq_table.description = f"staging table for `{self.table_full_name}`"
        bq_table.external_data_configuration = self._create_table_config(
            sample_filepath=sample_filepath
        )

        client.create_table(bq_table)
        log("Table created!")

    def append(self, source_filepath: str, partition: str):
        """
        Insere novos dados na tabela externa

        Args:
            source_filepath (str): Caminho dos dados locais
            partition (str): Partição Hive
        """
        st_obj = Storage(
            env=self.env,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            bucket_names=self.bucket_names,
        )

        st_obj.upload_file(
            mode="source",
            filepath=source_filepath,
            partition=partition,
        )
