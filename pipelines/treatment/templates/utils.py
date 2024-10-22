# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from google.cloud.dataplex_v1 import DataScanJob
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants
from pipelines.utils.discord import send_discord_embed_message
from pipelines.utils.utils import (
    convert_timezone,
    cron_get_last_date,
    cron_get_next_date,
)


class IncompleteDataError(Exception):
    """
    Erro para sinalizar falta dos dados necessários para executar o flow
    """


class DBTSelector:
    """
    Classe que representa um selector do DBT

    Args:
        name (str): nome do seletor no DBT
        schedule_cron (str): expressão cron que representa a frequência com que o seletor
            é executado
        initial_datetime (datetime): primeiro datetime que o selector deve ser executado (é usado
            na criação da primeira variável date_range_start)
        incremental_delay_hours (int): quantidade de horas que serão subtraídas do horário atual
            ao criar a variável date_range_end
    """

    def __init__(
        self,
        name: str,
        schedule_cron: str,
        initial_datetime: datetime,
        incremental_delay_hours: int = 0,
    ):
        self.name = name
        self.schedule_cron = schedule_cron
        self.incremental_delay_hours = incremental_delay_hours
        self.initial_datetime = initial_datetime

    def __getitem__(self, key):
        return self.__dict__[key]

    def get_last_materialized_datetime(self, env: str) -> datetime:
        """
        Pega o último datetime materializado no Redis

        Args:
            env (str): prod ou dev

        Returns:
            datetime: a data vinda do Redis
        """
        redis_key = f"{env}.selector_{self.name}"
        redis_client = get_redis_client(host="localhost")
        content = redis_client.get(redis_key)
        last_datetime = (
            self.initial_datetime
            if content is None
            else datetime.strptime(
                content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value],
                constants.MATERIALIZATION_LAST_RUN_PATTERN.value,
            )
        )

        return last_datetime

    def get_datetime_end(self, timestamp: datetime) -> datetime:
        """
        Calcula o datetime final da materialização com base em um timestamp

        Args:
            timestamp (datetime): datetime de referência

        Returns:
            datetime: datetime_end calculado
        """
        return timestamp - timedelta(hours=self.incremental_delay_hours)

    def is_up_to_date(self, env: str, timestamp: datetime) -> bool:
        """
        Confere se o selector está atualizado em relação a um timestamp

        Args:
            env (str): prod ou dev
            timestamp (datetime): datetime de referência

        Returns:
            bool: se está atualizado ou não
        """
        last_materialization = self.get_last_materialized_datetime(env=env)
        last_schedule = cron_get_last_date(cron_expr=self.schedule_cron, timestamp=timestamp)
        return last_materialization >= last_schedule - timedelta(hours=self.incremental_delay_hours)

    def get_next_schedule_datetime(self, timestamp: datetime) -> datetime:
        """
        Pega a próxima data de execução do selector em relação a um datetime
        com base no schedule_cron

        Args:
            timestamp (datetime): datetime de referência

        Returns:
            datetime: próximo datetime do cron
        """
        return cron_get_next_date(cron_expr=self.schedule_cron, timestamp=timestamp)

    def set_redis_materialized_datetime(self, env: str, timestamp: datetime):
        """
        Atualiza a timestamp de materialização no Redis

        Args:
            env (str): prod ou dev
            timestamp (datetime): data a ser salva no Redis
        """
        value = timestamp.strftime(constants.MATERIALIZATION_LAST_RUN_PATTERN.value)
        redis_key = f"{env}.selector_{self.name}"
        log(f"Saving timestamp {value} on key: {redis_key}")
        redis_client = get_redis_client(host="localhost")
        content = redis_client.get(redis_key)
        if not content:
            content = {constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value: value}
            redis_client.set(redis_key, content)
        else:
            if (
                convert_timezone(
                    datetime.strptime(
                        content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value],
                        constants.MATERIALIZATION_LAST_RUN_PATTERN.value,
                    )
                )
                < timestamp
            ):
                content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY.value] = value
                redis_client.set(redis_key, content)


def create_dataplex_log_message(dataplex_run: DataScanJob) -> str:
    """
    Cria a mensagem de log para os testes de qualidade de dados

    Args:
        dataplex_run(DataScanJob): Resultado da execução dos testes

    Returns:
        str: mensagem para ser exibida nos logs do Prefect
    """
    rule_count = len(dataplex_run.data_quality_result.rules)
    failed_rules = [r for r in dataplex_run.data_quality_result.rules if not r.passed]
    failed_rule_count = len(failed_rules)

    log_msg = f"""Resultado dos testes de qualidade de dados:
{len([r for r in dataplex_run.data_quality_result.rules if r.passed])}/{rule_count} Sucessos
{failed_rule_count}/{rule_count} Falhas
"""
    if failed_rule_count > 0:
        log_msg += "\n\nOs seguintes testes falharam:"
        for rule in failed_rules:
            log_msg += f"\n\t- {rule.rule.name}"
    return log_msg


def send_dataplex_discord_message(
    dataplex_check_id: str,
    dataplex_run: DataScanJob,
    timestamp: datetime,
    partitions: list,
):
    """
    Envia o relatório dos testes de qualidade de dados no Discord

    dataplex_check_id (str): ID do teste no Dataplex
    dataplex_run (DataScanJob): Resultado da execução dos testes
    timestamp (datetime): Timestamp de execução do flow
    partitions (list): Lista de partições testadas
    """
    msg_pattern = """**Partições:** {partitions}
**Porcentagem de registros com erro:** {error_ratio}%
**Query para verificação:** {failing_rows_query}
"""
    embed = [
        {
            "name": f"Regra: {r.rule.name}",
            "value": msg_pattern.format(
                partitions=partitions,
                error_ratio=(1 - round(r.pass_ratio, 2)) * 100,
                failing_rows_query=r.failing_rows_query,
            ),
        }
        for r in dataplex_run.data_quality_result.rules
    ]
    send_discord_embed_message(
        webhook_secret_key=constants.DATAPLEX_WEBHOOK.value,
        content=f"Alerta de qualidade de dados: {dataplex_check_id}",
        author_name="Dataplex Quality Checks",
        author_url=f"{constants.DATAPLEX_URL.value}/{dataplex_check_id}",
        embed_messages=embed,
        timestamp=timestamp,
    )
