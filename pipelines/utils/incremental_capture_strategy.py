# -*- coding: utf-8 -*-
"""Module to get incremental capture values"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Union

from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone

from pipelines.constants import constants
from pipelines.utils.fs import read_raw_data
from pipelines.utils.gcp import BQTable
from pipelines.utils.utils import isostr_to_datetime


@dataclass
class IncrementalInfo:
    start_value: Any
    end_value: Any
    execution_mode: str

    def __getitem__(self, key):
        return self.__dict__[key]


class IncrementalCaptureStrategy(ABC):
    """
    Classe base para criar estratégias de captura incremental
    Para criar uma nova estratégia incremental:
    1. herde essa classe
    2. Implemente os métodos:
        to_dict: Deve retornar um dicionário com uma chave (o nome da estratégia)
            e os valores do dicionário devem ser os argumentos para instânciar a classe
        _get_end_value: Deve receber um start_value m e retornar o valor final da captura
        get_value_to_save: Deve retornar o valor a ser salvo no Redis no final do Flow
        parse_redis_value: Deve receber um valor retornado do Redis e converter ele para o
            tipo que será usado no Flow

    Args:
        max_incremental_window: A janela máxima para calcular o valor final
        first_value (optional): O valor inicial para a primeira execução
    """

    def __init__(
        self,
        max_incremental_window: Any,
        first_value: Any = None,
    ) -> None:
        self._max_incremental_window = max_incremental_window
        self._first_value = first_value
        self._redis_key = None
        self.incremental_info = None

    def __getitem__(self, key):
        return self.__dict__[key]

    def initialize(
        self,
        table: BQTable,
        overwrite_start_value: Any = None,
        overwrite_end_value: Any = None,
    ):
        """
        Define o modo de execução e inicializa os valores iniciais e finais

        Args:
            table (BQTable): O objeto de tabela usada na extração
            overwrite_start_value (optional): Sobrescreve o valor inicial
                (deve ter o mesmo formato do valor retornado pelo Redis)
            overwrite_end_value (optional): Sobrescreve o valor final
                (deve ter o mesmo formato do valor retornado pelo Redis)
        """
        self._redis_key = f"{table.env}.{table.dataset_id}.{table.table_id}"

        last_redis_value = self.query_redis().get(constants.REDIS_LAST_CAPTURED_VALUE_KEY.value)

        execution_mode = (
            constants.MODE_FULL.value if last_redis_value is None else constants.MODE_INCR.value
        )

        if execution_mode == constants.MODE_FULL.value and self._first_value is not None:
            last_redis_value = self.parse_redis_value(self._first_value)

        else:
            last_redis_value = self.parse_redis_value(last_redis_value)

        start_value = (
            self.parse_redis_value(overwrite_start_value)
            if overwrite_start_value is not None
            else last_redis_value
        )

        end_value = (
            self.parse_redis_value(overwrite_end_value)
            if overwrite_end_value is not None
            else self._get_end_value(start_value=start_value)
        )

        if start_value is not None:
            assert start_value <= end_value, "start_value greater than end_value"

        self.incremental_info = IncrementalInfo(
            start_value=start_value,
            end_value=end_value,
            execution_mode=execution_mode,
        )

    @abstractmethod
    def to_dict(self) -> dict:
        """
        Retorna o dicionário para ser passado como parâmetro em um Flow

        Returns:
            dict: Dicionário com uma chave (o nome da estratégia)
            e os valores do dicionário devem ser os argumentos para instânciar a classe
        """

    @abstractmethod
    def _get_end_value(self, start_value: Any) -> Any:
        """
        Calcula o valor final com base no valor inicial

        Args:
            start_value: Valor inicial da captura
        Returns:
            Any: Valor final da captura
        """

    @abstractmethod
    def get_value_to_save(self) -> Any:
        """
        Retorna o valor para salvar no Redis
        """

    def query_redis(self) -> dict:
        """
        Retorna o valor salvo no Redis

        Returns:
            dict: o conteúdo da key no Redis
        """
        redis_client = get_redis_client()
        content = redis_client.get(self._redis_key)
        if content is None:
            content = {}
        return content

    @abstractmethod
    def parse_redis_value(self, value: Any) -> Any:
        """
        Converte o valor retornado do Redis no tipo a ser usado no Flow

        Args:
            value: valor a ser convertido

        Returns:
            Any: Valor convertido
        """

    def save_on_redis(self):
        """
        Salva o valor no Redis se ele for maior que o atual

        Args:
            value_to_save: Valor a ser salvo no Redis
        """
        value_to_save = self.get_value_to_save()
        log(f"Saving value {value_to_save} on Redis")
        content = self.query_redis()
        old_value = content.get(constants.REDIS_LAST_CAPTURED_VALUE_KEY.value)
        log(f"Value currently saved on key {self._redis_key} = {old_value}")

        if old_value is None:
            flag_save = True
        else:
            old_value = self.parse_redis_value(old_value)
            flag_save = self.parse_redis_value(value_to_save) > old_value

        if flag_save:
            redis_client = get_redis_client()
            content[constants.REDIS_LAST_CAPTURED_VALUE_KEY.value] = value_to_save

            redis_client.set(self._redis_key, content)
            log(f"[key: {self._redis_key}] Value {value_to_save} saved on Redis!")
        else:
            log("Value already saved greater than value to save, task skipped")


class IDIncremental(IncrementalCaptureStrategy):
    """
    Classe para fazer capturas incrementais com base em um ID sequencial inteiro

    Valor inicial: Valor salvo no Redis (tipo int)
    Valor final: Valor inicial + max_incremental_window (tipo int)

    Salva no Redis o último id presente na captura

    Args:
        max_incremental_window (int): Range máximo de ids a serem capturados
        id_column_name (str): Nome da coluna de ID
        first_value (optional): O valor inicial para a primeira execução
    """

    def __init__(
        self,
        max_incremental_window: int,
        id_column_name: str,
        first_value: int = None,
    ) -> None:
        super().__init__(
            max_incremental_window=max_incremental_window,
            first_value=first_value,
        )
        self.id_column_name = id_column_name
        self._raw_filepath = None

    def initialize(
        self,
        table: BQTable,
        overwrite_start_value: int = None,
        overwrite_end_value: int = None,
    ):
        """
        Executa o método da classe Base e pega o raw_filepath da tabela

        Args:
            table (BQTable): O objeto de tabela usada na extração
            overwrite_start_value (int, optional): Sobrescreve o valor inicial
            overwrite_end_value (int, optional): Sobrescreve o valor final
        """
        super().initialize(
            table=table,
            overwrite_start_value=overwrite_start_value,
            overwrite_end_value=overwrite_end_value,
        )
        self._raw_filepath = table.raw_filepath

    def to_dict(self) -> dict:
        """
        Converte o objeto em um dicionário para ser passado como parâmetro no Flow

        Returns:
            dict: Dicionário com a key "id" e o valor contendo argumentos para intanciação
        """
        return {
            "id": {
                "max_incremental_window": self._max_incremental_window,
                "first_value": self._first_value,
                "id_column_name": self.id_column_name,
            }
        }

    def _get_end_value(self, start_value: int) -> int:
        """
        Calcula o valor final
        """
        if start_value is not None:
            return start_value + int(self._max_incremental_window)

    def get_value_to_save(self) -> int:
        """
        Busca no arquivo raw o último ID capturado
        """
        df = read_raw_data(filepath=self._raw_filepath)
        return df[self.id_column_name].dropna().astype(str).str.replace(".0", "").astype(int).max()

    def parse_redis_value(self, value: Union[int, str]) -> int:
        """
        Converte o valor para inteiro

        Args:
            value (Union[int, str]): Valor a ser convertido
        Returns:
            int: Valor convertido para inteiro
        """
        if value is not None:
            value = int(value)

        return value


class DatetimeIncremental(IncrementalCaptureStrategy):
    """
    Classe para fazer capturas incrementais com base em uma data

    Valor inicial: Última data salva no Redis (tipo datetime)
    Valor final: timestamp da tabela ou
        valor inicial + max_incremental_window (caso seja menor que a timestamp)
        (tipo datetime)

    Salva no Redis o valor final

    Args:
        max_incremental_window (dict): Dicionário com os argumentos de timedelta
            que representam o range máximo de datas a ser capturado
            (ex.: {"days": 1} captura no maximo 1 dia depois da data inicial)
        first_value (str, optional): O valor inicial para a primeira execução
            (deve ser uma string de datetime no formato iso)
    """

    def __init__(
        self,
        max_incremental_window: dict,
        first_value: str = None,
    ) -> None:
        super().__init__(
            max_incremental_window=max_incremental_window,
            first_value=first_value,
        )
        self._timestamp = None

    def initialize(
        self,
        table: BQTable,
        overwrite_start_value: str = None,
        overwrite_end_value: str = None,
    ):
        """
        Executa o método da classe Base e pega o timestamp da tabela

        Args:
            table (BQTable): O objeto de tabela usada na extração
            overwrite_start_value (str, optional): Sobrescreve o valor inicial
                (deve ser uma string de datetime no formato iso)
            overwrite_end_value (str, optional): Sobrescreve o valor final
                (deve ser uma string de datetime no formato iso)
        """
        self._timestamp = table.timestamp
        return super().initialize(
            table=table,
            overwrite_start_value=overwrite_start_value,
            overwrite_end_value=overwrite_end_value,
        )

    def to_dict(self) -> dict:
        """
        Converte o objeto em um dicionário para ser passado como parâmetro no Flow

        Returns:
            dict: Dicionário com a key "datetime" e o valor contendo argumentos para intanciação
        """
        return {
            "datetime": {
                "max_incremental_window": self._max_incremental_window,
                "first_value": self._first_value,
            }
        }

    def _get_end_value(self, start_value: datetime) -> datetime:
        """
        Calcula o valor final
        """
        if start_value is not None:
            end_value = min(
                self._timestamp, start_value + timedelta(**self._max_incremental_window)
            )
        else:
            end_value = self._timestamp

        if not end_value.tzinfo:
            end_value = end_value.replace(tzinfo=timezone("UTC"))
        else:
            end_value = end_value.astimezone(tz=timezone("UTC"))

        return end_value

    def get_value_to_save(self) -> str:
        """
        Transforma o valor final em string para salvar no Redis
        """
        return self.incremental_info.end_value.isoformat()

    def parse_redis_value(self, value: Union[datetime, str]) -> datetime:
        """
        Converte o valor em um datetime com a timezone UTC

        Args:
            value (Union[datetime, str]): Valor a ser convertido
        Returns:
            datetime: Valor convertido para datetime UTC
        """
        if value is not None:
            if isinstance(value, str):
                value = isostr_to_datetime(value)
            elif isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone("UTC"))
                else:
                    value = value.astimezone(tz=timezone("UTC"))
            else:
                raise ValueError("value must be str or datetime")

        return value


def incremental_strategy_from_dict(strategy_dict: dict) -> IncrementalCaptureStrategy:
    """
    Instancia uma IncrementalCaptureStrategy com base em um dicionário

    Args:
        strategy_dict (dict): Dicionário com uma key (tipo da incremental: id ou datetime)
            e valores sendo os argumentos para passar ao construtor do objeto

    Returns:
        IncrementalCaptureStrategy: classe concreta instanciada
    """
    incremental_type = list(strategy_dict.keys())[0]
    class_map = {
        "id": IDIncremental,
        "datetime": DatetimeIncremental,
    }
    return class_map[incremental_type](**strategy_dict[incremental_type])
