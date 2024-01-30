# -*- coding: utf-8 -*-
"""Module to get data from GCS"""
from pipelines.utils.capture.base import DataExtractor
from pipelines.utils.fs import get_filetype
from pipelines.utils.gcp import Storage


class GCSExtractor(DataExtractor):
    def __init__(
        self,
        env: str,
        folder: str,
        filename: str,
        save_path: str,
        bucket_name: str = None,
    ) -> None:
        super().__init__(save_path=save_path)
        filetype = get_filetype(filepath=save_path)
        self.complete_filename = f"{filename}.{filetype}"
        self.storage = Storage(env=env, dataset_id=folder, bucket_name=bucket_name)

    def _get_data(self) -> str:
        data = self.storage.get_blob_string(mode="upload", filename=self.complete_filename)

        return data
