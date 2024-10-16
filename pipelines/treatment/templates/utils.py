# -*- coding: utf-8 -*-
from datetime import datetime

from google.cloud.dataplex_v1 import DataScanJob
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants
from pipelines.utils.discord import send_discord_embed_message
from pipelines.utils.utils import cron_get_next_date


class DBTSelector:
    def __init__(
        self,
        name: str,
        schedule_cron: str,
        incremental_delay_hours: int,
        initial_datetime: datetime,
    ):
        self.name = name
        self.schedule_cron = schedule_cron
        self.incremental_delay_hours = incremental_delay_hours
        self.initial_datetime = initial_datetime

    def __getitem__(self, key):
        return self.__dict__[key]

    def get_last_materialized_datetime(self, env: str) -> datetime:
        redis_key = f"{env}.selector_{self.name}"
        redis_client = get_redis_client()
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

    def get_next_schedule_datetime(self, timestamp: datetime) -> datetime:
        return cron_get_next_date(cron_expr=self.schedule_cron, timestamp=timestamp)

    def set_redis_materialized_datetime(self, env: str, timestamp: datetime):
        value = timestamp.strftime(constants.MATERIALIZATION_LAST_RUN_PATTERN.value)
        redis_key = f"{env}.selector_{self.name}"
        log(f"Saving timestamp {value} on key: {redis_key}")
        redis_client = get_redis_client()
        content = redis_client.get(redis_key)
        if not content:
            content = {}
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
