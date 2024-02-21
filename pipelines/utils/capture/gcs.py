# -*- coding: utf-8 -*-
"""Module to get data from GCS"""
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.capture.base import DataExtractor
from pipelines.utils.fs import get_filetype
from pipelines.utils.gcp import Storage


class GCSExtractor(DataExtractor):
    """
    Classe para extrair dados do GCS

    Args:
        env (str): dev ou prod
        folder (str): pasta que está o arquivo
        filename (str): nome do arquivo sem extensão
        save_filepath (str): Caminho para salvar o arquivo
            (deve ter a mesma extensão do arquivo no GCS)
        bucket_name (str): Nome do bucket no GCS
    """

    def __init__(
        self,
        env: str,
        folder: str,
        filename: str,
        save_filepath: str,
        bucket_name: str = None,
    ) -> None:
        super().__init__(save_filepath=save_filepath)
        filetype = get_filetype(filepath=save_filepath)
        self.complete_filename = f"{filename}.{filetype}"
        self.storage = Storage(env=env, dataset_id=folder, bucket_name=bucket_name)

    def _get_data(self) -> str:
        """Baixa o arquivo como string

        Returns:
            str: conteúdo do arquivo
        """
        log(f"Getting file: {self.complete_filename}")
        data = self.storage.get_blob_string(mode="upload", filename=self.complete_filename)

        return data
