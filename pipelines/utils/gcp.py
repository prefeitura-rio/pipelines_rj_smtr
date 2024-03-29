# -*- coding: utf-8 -*-
"""Module to interact with GCP"""
import csv
import inspect
import io
import zipfile
from dataclasses import dataclass
from datetime import datetime
from mimetypes import MimeTypes
from pathlib import Path
from typing import Type, TypeVar, Union

import basedosdados as bd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery.external_config import HivePartitioningOptions
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.fs import create_capture_filepath, create_partition

T = TypeVar("T")
# Set BD config to run on cloud #
bd.config.from_file = True


@dataclass
class GCPBase:
    dataset_id: str
    table_id: str
    bucket_names: dict
    env: str

    def __post_init__(self):
        if self.bucket_names is None:
            self.bucket_name = constants.DEFAULT_BUCKET_NAME.value[self.env]
        else:
            self.bucket_name = self.bucket_names[self.env]

    def __getitem__(self, key):
        return self.__dict__[key]

    def client(self, service: str) -> Union[storage.Client, bigquery.Client]:
        service_map = {"storage": storage.Client, "bigquery": bigquery.Client}
        return service_map[service](project=constants.PROJECT_NAME.value[self.env])

    def transfer_gcp_obj(self, target_class: Type[T], **additional_kwargs) -> T:
        base_args = list(inspect.signature(GCPBase).parameters.keys())
        init_args = list(inspect.signature(target_class).parameters.keys())
        kwargs = {k: self[k] for k in init_args if k in base_args} | additional_kwargs
        return target_class(**kwargs)


class Storage(GCPBase):
    def __init__(
        self,
        env: str,
        dataset_id: str,
        table_id: str = None,
        bucket_names: str = None,
    ):
        super().__init__(
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_names=bucket_names,
            env=env,
        )

        self.bucket = self.client("storage").bucket(self.bucket_name)

    def create_blob_name(
        self,
        mode: str,
        filename: str = None,
        filetype: str = None,
        partition: str = None,
    ) -> str:
        blob_name = f"{mode}/{self.dataset_id}"
        if self.table_id is not None:
            blob_name += f"/{self.table_id}"

        if partition is not None:
            partition = partition.strip("/")
            blob_name += f"/{partition}"

        if filename is not None:
            blob_name += f"/{filename}"

            if filetype is not None:
                blob_name += f".{filetype}"
        else:
            blob_name += "/"

        return blob_name

    def _check_mode(self, mode: str):
        accept = ["upload", "raw", "source"]
        if mode not in accept:
            raise ValueError(f"mode must be: {', '.join(accept)}. Received {mode}")

    def upload_file(
        self,
        mode: str,
        filepath: Union[str, Path],
        partition: str = None,
        if_exists="replace",
        chunk_size=None,
        **upload_kwargs,
    ):
        filepath = Path(filepath)

        if filepath.is_dir():
            raise IsADirectoryError("filepath is a directory")

        filename_parts = filepath.name.rsplit(".", 1)
        filetype = filename_parts[1] if len(filename_parts) > 1 else None
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename_parts[0],
            filetype=filetype,
        )

        blob = self.bucket.blob(blob_name, chunk_size=chunk_size)

        if not blob.exists() or if_exists == "replace":
            log(f"Uploading file {filepath} to {self.bucket.name}/{blob_name}")
            upload_kwargs["timeout"] = upload_kwargs.get("timeout", None)

            blob.upload_from_filename(str(filepath), **upload_kwargs)
            log("File uploaded!")

        elif if_exists == "pass":
            log("Blob already exists skipping upload")

        else:
            raise FileExistsError("Blob already exists")

    def get_blob_obj(
        self,
        mode: str,
        filename: str,
        filetype: str = None,
        partition: str = None,
    ):
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename,
            filetype=filetype,
        )
        return self.bucket.get_blob(blob_name=blob_name)

    def get_blob_bytes(
        self,
        mode: str,
        filename: str,
        filetype: str = None,
        partition: str = None,
    ) -> bytes:
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename,
            filetype=filetype,
        )
        return self.bucket.get_blob(blob_name=blob_name).download_as_bytes()

    def get_blob_string(
        self,
        mode: str,
        filename: str,
        filetype: str = None,
        partition: str = None,
    ) -> str:
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename,
            filetype=filetype,
        )
        return self.bucket.get_blob(blob_name=blob_name).download_as_text()

    def unzip_file(self, mode: str, zip_filename: str, unzip_to: str):
        data = self.get_blob_bytes(mode=mode, filename=zip_filename)
        mime = MimeTypes()
        with zipfile.ZipFile(io.BytesIO(data), "r") as zipped_file:
            for name in zipped_file.namelist():
                unzipped_data = zipped_file.read(name=name)

                filename_parts = name.rsplit(".", 1)

                filetype = filename_parts[1] if len(filename_parts) > 1 else None

                blob_name = self.create_blob_name(
                    mode=mode,
                    partition=unzip_to,
                    filename=filename_parts[0],
                    filetype=filetype,
                )

                self.bucket.blob(blob_name).upload_from_string(
                    data=unzipped_data,
                    content_type=mime.guess_type(name)[0],
                )

    def move_folder(
        self,
        new_storage: "Storage",
        old_mode: str,
        new_mode: str,
        partitions: Union[str, list[str]] = None,
    ):
        partitions = (
            [partitions] if isinstance(partitions, str) or partitions is None else partitions
        )

        blobs = []

        for partition in partitions:
            blob_prefix = self.create_blob_name(mode=old_mode, partition=partition)
            source_blobs = list(self.bucket.list_blobs(prefix=blob_prefix))

            blob_mapping = [
                {
                    "source_blob": blob,
                    "new_name": blob.name.replace(
                        blob_prefix,
                        new_storage.create_blob_name(mode=new_mode, partition=partition),
                        1,
                    ),
                }
                for blob in source_blobs
                if not blob.name.endswith("/")
            ]

            blobs += blob_mapping

        if new_storage.bucket_name != self.bucket_name:
            for blob in blobs:
                source_blob: storage.Blob = blob["source_blob"]
                self.bucket.copy_blob(source_blob, new_storage.bucket, new_name=blob["new_name"])
                source_blob.delete()
        else:
            for blob in blobs:
                self.bucket.rename_blob(blob["source_blob"], new_name=blob["new_name"])


class Dataset(GCPBase):
    def __init__(self, dataset_id: str, env: str, location: str = "southamerica-east1") -> None:
        super().__init__(
            dataset_id=dataset_id,
            table_id="",
            bucket_names=None,
            env=env,
        )
        self.location = location

    def exists(self) -> bool:
        try:
            self.client("bigquery").get_dataset(self.dataset_id)
            return True
        except NotFound:
            return False

    def create(self):
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
    def __init__(
        self,
        env: str,
        dataset_id: str,
        table_id: str,
        bucket_names: dict = None,
        timestamp: datetime = None,
        partition_date_only: bool = False,
        raw_filetype: str = "json",
    ) -> None:
        super().__init__(
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_names=bucket_names,
            env=env,
        )

        self.table_full_name = (
            f"{constants.PROJECT_NAME.value[env]}.{self.dataset_id}.{self.table_id}"
        )
        if timestamp is None:
            self.partition = None
            self.raw_filepath = None
            self.source_filepath = None
        else:
            self.partition = create_partition(
                timestamp=timestamp,
                partition_date_only=partition_date_only,
            )

            filepaths = create_capture_filepath(
                dataset_id=dataset_id,
                table_id=table_id,
                timestamp=timestamp,
                raw_filetype=raw_filetype,
                partition=self.partition,
            )

            self.raw_filepath = filepaths.get("raw")
            self.source_filepath = filepaths.get("source")

        self.timestamp = timestamp

    def _create_table_schema(self) -> list[bigquery.SchemaField]:
        log("Creating table schema...")
        with open(self.source_filepath, "r", encoding="utf-8") as fi:
            columns = next(csv.reader(fi))

        log(f"Columns found: {columns}")
        schema = [
            bigquery.SchemaField(name=col, field_type="STRING", description=None) for col in columns
        ]
        log("Schema created!")
        return schema

    def _create_table_config(self) -> bigquery.ExternalConfig:
        if self.source_filepath is None:
            raise AttributeError("source_filepath is None")

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

    def upload_raw_file(self):
        if self.raw_filepath is None:
            raise AttributeError("raw_filepath is None")

        st_obj = self.transfer_gcp_obj(target_class=Storage)

        st_obj.upload_file(
            mode="raw",
            filepath=self.raw_filepath,
            partition=self.partition,
        )

    def exists(self) -> bool:
        try:
            return bool(self.client("bigquery").get_table(self.table_full_name))
        except NotFound:
            return False

    def create(self, location: str = "southamerica-east1"):
        log(f"Creating External Table: {self.table_full_name}")
        self.append()
        dataset_obj = self.transfer_gcp_obj(target_class=Dataset, location=location)
        dataset_obj.create()

        client = self.client("bigquery")

        bq_table = bigquery.Table(self.table_full_name)
        bq_table.description = f"staging table for `{self.table_full_name}`"
        bq_table.external_data_configuration = self._create_table_config()

        client.create_table(bq_table)
        log("Table created!")

    def append(self):
        if self.source_filepath is None:
            raise ValueError("source_filepath is None")

        st_obj = self.transfer_gcp_obj(target_class=Storage)

        st_obj.upload_file(
            mode="source",
            filepath=self.source_filepath,
            partition=self.partition,
        )

    def get_table_min_max_value(self, field_name: str, kind: str):
        log(f"Getting {kind} value for {self.table_id}")
        query = f"""
        SELECT
            {kind}({field_name})
        FROM {self.table_full_name}
        """
        result = bd.read_sql(query=query)

        return result.iloc[0][0]
