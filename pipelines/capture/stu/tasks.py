# -*- coding: utf-8 -*-
"""Tasks for pipelines.capture.stu"""

from pipelines.utils.extractors.gcs import GCSExtractor
from pipelines.utils.gcp import Storage
from pipelines.capture.stu.constants import constants as stu_constants
from pipelines.utils.prefect import extractor_task


@extractor_task
def create_extractor_stu(
    env: str, save_filepath: str, dataset_id: str, data_extractor_params: dict, table_id: str
) -> GCSExtractor:
    """Cria o extrator de dados para capturas de STU"""
    bucket_names = stu_constants.STU_BUCKET_NAME.value
    storage = Storage(env=env, dataset_id=dataset_id, table_id=table_id, bucket_names=bucket_names)
    blob_list = storage.list_blobs(mode="upload")

    filename = [
        blob.name.split("/")[-1].replace(".txt", "")
        for blob in blob_list
        if blob.name.endswith(f"{data_extractor_params['data_versao_stu'].replace('-', '')}.txt")
    ]

    return GCSExtractor(
        env=env,
        folder=dataset_id,
        filename=filename,
        save_filepath=save_filepath,
        bucket_names=bucket_names,
        multiple_file_reader_args={
            "sep": ";",
            "decimal": ",",
            "encoding": "latin-1",
            "dtype": "object",
        },
    )
