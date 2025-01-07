# -*- coding: utf-8 -*-
"""Classes e funções úteis para captura de dados"""
import csv
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants
from pipelines.utils.gcp.bigquery import BQTable, Dataset
from pipelines.utils.gcp.storage import Storage
from pipelines.utils.prefect import flow_is_running_local
from pipelines.utils.utils import convert_timezone, cron_date_range, cron_get_last_date


class SourceTable(BQTable):
    """
    Classe pai para manipular dados capturados pelos flows

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
        raw_filetype: str = "json",
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
        self.first_timestamp = convert_timezone(first_timestamp)
        self.pretreatment_reader_args = pretreatment_reader_args
        self.pretreat_funcs = pretreat_funcs or []
        self.schedule_cron = schedule_cron

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

    def is_up_to_date(self, env: str, timestamp: datetime, retroactive_days: int) -> bool:
        """
        Confere se o source está atualizado em relação a um timestamp

        Args:
            env (str): prod ou dev
            timestamp (datetime): datetime de referência
            retroactive_days (int): número de dias anteriores à timestamp informada que a busca
                irá verificar

        Returns:
            bool: se está atualizado ou não
        """

        return True

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


class DefaultSourceTable(SourceTable):
    """
    Classe para manipular dados capturados pelos flows com captura e recaptura

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
    ) -> None:
        super().__init__(
            source_name=source_name,
            table_id=table_id,
            first_timestamp=first_timestamp,
            schedule_cron=schedule_cron,
            primary_keys=primary_keys,
            pretreatment_reader_args=pretreatment_reader_args,
            pretreat_funcs=pretreat_funcs,
            bucket_names=bucket_names,
            partition_date_only=partition_date_only,
            raw_filetype=raw_filetype,
        )
        self.max_recaptures = max_recaptures

    def get_uncaptured_timestamps(
        self,
        timestamp: datetime,
        retroactive_days: int = 2,
        env: str = None,
    ) -> list:
        """
        Retorna todas as timestamps não capturadas até um datetime

        Args:
            timestamp (datetime): filtro limite para a busca
            retroactive_days (int): número de dias anteriores à timestamp informada que a busca
                irá verificar
            env (str): prod ou dev

        Returns:
            list: Lista com as timestamps não capturadas
        """
        env = env or self.env
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
                if ".csv" in b.name
            ]

        return [d for d in full_range if d not in files][: self.max_recaptures]

    def is_up_to_date(self, env: str, timestamp: datetime, **kwargs) -> bool:
        """
        Confere se o source está atualizado em relação a um timestamp

        Args:
            env (str): prod ou dev
            timestamp (datetime): datetime de referência
            retroactive_days (int): número de dias anteriores à timestamp informada que a busca
                irá verificar

        Returns:
            bool: se está atualizado ou não
        """
        uncaptured_timestamps = self.get_uncaptured_timestamps(
            timestamp=timestamp,
            retroactive_days=kwargs["retroactive_days"],
            env=env,
        )

        return len(uncaptured_timestamps) == 0


class DateRangeSourceTable(SourceTable):
    """
    Classe para manipular dados capturados pelos flows
    salvando a última data captura no Redis

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
        max_capture_hours (int): número máximo de horas que podem ser capturadas de uma vez
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
        max_capture_hours: int = 24,
        raw_filetype: str = "json",
    ) -> None:
        super().__init__(
            source_name=source_name,
            table_id=table_id,
            first_timestamp=first_timestamp,
            schedule_cron=schedule_cron,
            primary_keys=primary_keys,
            pretreatment_reader_args=pretreatment_reader_args,
            pretreat_funcs=pretreat_funcs,
            bucket_names=bucket_names,
            partition_date_only=partition_date_only,
            raw_filetype=raw_filetype,
        )
        self.max_capture_hours = max_capture_hours

    def _get_redis_client(self):
        if flow_is_running_local():
            return get_redis_client(host="localhost")
        return get_redis_client()

    def get_last_captured_datetime(self, env: str = None) -> datetime:
        """
        Pega o último datetime materializado no Redis

        Args:
            env (str): prod ou dev

        Returns:
            datetime: a data vinda do Redis
        """
        env = env or self.env
        redis_key = f"{env}.{self.dataset_id}.{self.table_id}"
        redis_client = self._get_redis_client()
        content = redis_client.get(redis_key)
        last_datetime = (
            self.first_timestamp
            if content is None
            else datetime.strptime(
                content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value],
                constants.MATERIALIZATION_LAST_RUN_PATTERN.value,
            )
        )

        last_datetime = convert_timezone(timestamp=last_datetime)

        return last_datetime

    def get_capture_date_range(self, timestamp: datetime) -> dict:
        date_range_start = self.get_last_captured_datetime()
        date_range_end = min(date_range_start + timedelta(hours=self.max_capture_hours), timestamp)

        return {"date_range_start": date_range_start, "date_range_end": date_range_end}

    def is_up_to_date(self, env: str, timestamp: datetime, **kwargs) -> bool:
        """
        Confere se o source está atualizado em relação a um timestamp

        Args:
            env (str): prod ou dev
            timestamp (datetime): datetime de referência

        Returns:
            bool: se está atualizado ou não
        """

        return timestamp >= self.get_last_captured_datetime(env=env)

    def set_redis_last_captured_datetime(self, timestamp: datetime):
        """
        Atualiza a timestamp de captura no Redis

        Args:
            timestamp (datetime): data a ser salva no Redis
        """
        value = timestamp.strftime(constants.MATERIALIZATION_LAST_RUN_PATTERN.value)
        redis_key = f"{self.env}.{self.dataset_id}.{self.table_id}"
        redis_client = self._get_redis_client()
        content = redis_client.get(redis_key)
        log(f"Salvando timestamp {value} na key: {redis_key}")
        if not content:
            content = {constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value: value}
            redis_client.set(redis_key, content)
            log("Timestamp salva!")
        else:
            last_timestamp = convert_timezone(
                datetime.strptime(
                    content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value],
                    constants.MATERIALIZATION_LAST_RUN_PATTERN.value,
                )
            )
            log(f"Última timestamp salva no Redis: {last_timestamp}")

            if last_timestamp < timestamp:
                content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value] = value
                redis_client.set(redis_key, content)
                log("Timestamp salva!")
