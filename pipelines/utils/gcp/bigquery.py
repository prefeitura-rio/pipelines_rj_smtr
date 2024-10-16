# -*- coding: utf-8 -*-
"""Módulo para interagir com tabelas e datasets do BigQuery"""

from datetime import datetime, timedelta
from typing import Callable

import basedosdados as bd
import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from prefeitura_rio.pipelines_utils.logging import log
from pytz import timezone

from pipelines.constants import constants
from pipelines.utils.gcp.base import GCPBase
from pipelines.utils.gcp.storage import Storage
from pipelines.utils.utils import cron_date_range


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
    def __init__(  # pylint: disable=R0913
        self, env: str, dataset_id: str, table_id: str, bucket_names: dict = None
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


class SourceTable(BQTable):
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
    ) -> None:
        self.source_name = source_name
        super().__init__(
            dataset_id="source_" + source_name,
            table_id=table_id,
            bucket_names=bucket_names,
            env="dev",
            schedule_cron=schedule_cron,
        )
        self.raw_filetype = raw_filetype
        self.primary_keys = primary_keys
        self.partition_date_only = partition_date_only
        self.max_recaptures = max_recaptures
        self.first_timestamp = first_timestamp
        self.pretreatment_reader_args = pretreatment_reader_args
        self.pretreat_funcs = pretreat_funcs or []
        self.schedule_cron = schedule_cron

    def _create_table_schema(self) -> list[bigquery.SchemaField]:
        log("Creating table schema...")
        columns = self.primary_keys + ["content", "timestamp_captura"]

        log(f"Columns: {columns}")
        schema = [
            bigquery.SchemaField(name=col, field_type="STRING", description=None) for col in columns
        ]
        log("Schema created!")
        return schema

    def _create_table_config(self) -> bigquery.ExternalConfig:

        external_config = bigquery.ExternalConfig("CSV")
        external_config.options.skip_leading_rows = 1
        external_config.options.allow_quoted_newlines = True
        external_config.autodetect = False
        external_config.schema = self._create_table_schema()
        external_config.options.field_delimiter = ","
        external_config.options.allow_jagged_rows = False

        uri = f"gs://{self.bucket_name}/source/{self.dataset_id}/{self.table_id}/*"
        external_config.source_uris = uri
        hive_partitioning = HivePartitioningOptions()
        hive_partitioning.mode = "STRINGS"
        hive_partitioning.source_uri_prefix = uri.replace("*", "")
        external_config.hive_partitioning = hive_partitioning

        return external_config

    def get_uncaptured_timestamps(self, timestamp: datetime, retroactive_days: int = 1) -> list:
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
                datetime.strptime(b.name.split("/")[-1], "%Y-%m-%d-%H-%M-%S.csv")
                for b in st.bucket.list_blobs(prefix=prefix)
            ]
        tz = timezone(constants.TIMEZONE.value)
        return [tz.localize(d) for d in full_range if d not in files][: self.max_recaptures]

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

    def create(self, location: str = "US"):
        log(f"Creating External Table: {self.table_full_name}")
        dataset_obj = Dataset(dataset_id=self.dataset_id, env=self.env, location=location)
        dataset_obj.create()

        client = self.client("bigquery")

        bq_table = bigquery.Table(self.table_full_name)
        bq_table.description = f"staging table for `{self.table_full_name}`"
        bq_table.external_data_configuration = self._create_table_config()

        client.create_table(bq_table)
        log("Table created!")

    def append(self, source_filepath: str, partition: str):

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
