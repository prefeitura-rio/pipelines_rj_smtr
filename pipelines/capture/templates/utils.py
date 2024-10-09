# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from prefeitura_rio.pipelines_utils.logging import log
from pytz import timezone

from pipelines.constants import constants
from pipelines.utils.fs import create_capture_filepath, create_partition
from pipelines.utils.gcp import BQTable, Dataset, Storage


class Source(BQTable):
    def __init__(
        self,
        dataset_id: str,
        table_id: str,
        first_timestamp: datetime,
        primary_keys: list[str] = None,
        pretreatment_reader_args: dict = None,
        interval_minutes: int = 1,
        bucket_names: dict = None,
        partition_date_only: bool = False,
        recapture_days: int = 1,
        raw_filetype: str = "json",
    ) -> None:
        super().__init__(
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_names=bucket_names,
            env="dev",
        )
        self.raw_filetype = raw_filetype
        self.primary_keys = primary_keys
        self.partition_date_only = partition_date_only
        self.interval_minutes = interval_minutes
        self.recapture_days = recapture_days
        self.first_timestamp = first_timestamp
        self.pretreatment_reader_args = pretreatment_reader_args

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
        st = self.transfer_gcp_obj(target_class=Storage)
        initial_timestamp = max(timestamp - timedelta(days=retroactive_days), self.first_timestamp)
        full_range = pd.date_range(initial_timestamp, timestamp, freq=f"{self.interval_minutes}min")
        days_to_check = pd.date_range(initial_timestamp.date(), timestamp.date())

        files = []
        for day in days_to_check:
            prefix = f"source/{self.dataset_id}/{self.table_id}/data={day.date().isoformat()}/"
            files = files + [b.name.split("/")[-1] for b in st.bucket.list_blobs(prefix=prefix)]
        tz = timezone(constants.TIMEZONE.value)
        return [
            tz.localize(d)
            for d in full_range.difference(
                pd.to_datetime(files, format="%Y-%m-%d-%H-%M-%S.csv", utc=False)
            ).to_pydatetime()
        ]

    def get_filepaths(self, timestamp: datetime) -> dict[str, str]:
        partition = create_partition(
            timestamp=timestamp,
            partition_date_only=self.partition_date_only,
        )

        filepaths = create_capture_filepath(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            timestamp=timestamp,
            raw_filetype=self.raw_filetype,
            partition=partition,
        )

        return {"partition": partition, "filepaths": filepaths}

    def upload_raw_file(self, raw_filepath: str, partition: str):

        st_obj = self.transfer_gcp_obj(target_class=Storage)

        st_obj.upload_file(
            mode="raw",
            filepath=raw_filepath,
            partition=partition,
        )

    def create(self, location: str = "southamerica-east1"):
        log(f"Creating External Table: {self.table_full_name}")
        dataset_obj = self.transfer_gcp_obj(target_class=Dataset, location=location)
        dataset_obj.create()

        client = self.client("bigquery")

        bq_table = bigquery.Table(self.table_full_name)
        bq_table.description = f"staging table for `{self.table_full_name}`"
        bq_table.external_data_configuration = self._create_table_config()

        client.create_table(bq_table)
        log("Table created!")

    def append(self, source_filepath: str, partition: str):

        st_obj = self.transfer_gcp_obj(target_class=Storage)

        st_obj.upload_file(
            mode="source",
            filepath=source_filepath,
            partition=partition,
        )
