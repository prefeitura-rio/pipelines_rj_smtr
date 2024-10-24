# -*- coding: utf-8 -*-
"""Module to get data from GCS"""
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.gcp.storage import Storage


def get_raw_gcs(
    env: str,
    folder: str,
    filename: str,
    filetype: str,
    bucket_names: dict[str] = None,
):
    """
    Captura dados do GCS

    Args:
        env (str): dev ou prod
        folder (str): pasta que está o arquivo
        filename (str): nome do arquivo sem extensão
        filetype (str): Extensão do arquivo
        bucket_name (str): Nome do bucket no GCS
    """
    storage = Storage(env=env, dataset_id=folder, bucket_names=bucket_names)
    complete_filename = f"{filename}.{filetype}"
    log(f"Getting file: {complete_filename}")
    return storage.get_blob_string(mode="upload", filename=complete_filename)
