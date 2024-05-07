# -*- coding: utf-8 -*-
"""Module to get data from GCS"""
from io import StringIO
import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.extractors.base import DataExtractor
from pipelines.utils.fs import get_filetype
from pipelines.utils.gcp import Storage


class GCSExtractor(DataExtractor):
    """
    Classe para extrair dados do GCS

    Args:
        env (str): dev ou prod
        folder (str): pasta que está o arquivo
        filename (str | list): nome ou lista de nomes dos arquivos sem extensão
            (se for uma lista deve ser CSV ou TXT com as mesmas colunas)
        save_filepath (str): Caminho para salvar o arquivo
            (deve ter a mesma extensão do arquivo no GCS)
        bucket_name (str): Nome do bucket no GCS
    """

    def __init__(
        self,
        env: str,
        folder: str,
        filename: str | list[str],
        save_filepath: str,
        bucket_names: dict[str] = None,
        multiple_file_reader_args: dict = None,
    ) -> None:
        super().__init__(save_filepath=save_filepath)
        self.multiple_file_reader_args = multiple_file_reader_args or {}
        filetype = get_filetype(filepath=save_filepath)
        if isinstance(filename, list):
            if filetype not in ["csv", "txt"]:
                raise ValueError(
                    "caso filename seja uma lista, o tipo de arquivo deve ser TXT ou CSV"
                )
            self.complete_filename = [f"{f}.{filetype}" for f in filename]
        else:
            self.complete_filename = f"{filename}.{filetype}"
        self.storage = Storage(env=env, dataset_id=folder, bucket_names=bucket_names)

    def _get_data(self) -> str:
        """Baixa o arquivo como string

        Returns:
            str: conteúdo do arquivo
        """
        if isinstance(self.complete_filename, list):
            all_df = []
            for name in self.complete_filename:
                log(f"Getting file: {name}")
                file_data = StringIO(self.storage.get_blob_string(mode="upload", filename=name))
                df = pd.read_csv(file_data, **self.multiple_file_reader_args)
                df["_filename"] = name
                all_df.append(df)
            data = pd.concat(all_df).to_csv(index=False, header=True)
        else:
            log(f"Getting file: {self.complete_filename}")
            data = self.storage.get_blob_string(mode="upload", filename=self.complete_filename)

        return data
