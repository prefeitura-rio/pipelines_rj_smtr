# -*- coding: utf-8 -*-
"""Tasks de tratamento dos dados financeiros"""
from datetime import date, datetime, timedelta
from typing import Optional

import basedosdados as bd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants as smtr_constants
from pipelines.utils.utils import convert_timezone


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def get_ordem_quality_check_start_datetime(
    env: str,
    dataset_id: str,
    table_id: str,
    start_datetime: Optional[str],
    partitions: Optional[list],
) -> Optional[datetime]:
    """
    Consulta o Redis para pegar o datetime inicial para fazer o teste
    de qualidade dos dados da ordem de pagamento

    Args:
        env (str): prod ou dev
        dataset_id (str): dataset_id no BigQuery,
        table_id (str): table_id no BigQuery,
        start_datetime (str): Parâmetro do flow para definir o valor manualmente
        partitions (list): Parâmetro do flow para definir as partições consultadas manualmente

    returns:
        Optional[datetime]: datetime salvo no redis, valor do
        parâmetro start_datetime ou None caso os dois anteriores sejam nulos
        ou se o parâmetro partitions não for nulo
    """
    if partitions is not None:
        return
    if start_datetime is not None:
        return convert_timezone(datetime.fromisoformat(start_datetime))

    redis_key = f"{env}.{dataset_id}.{table_id}.data_quality_check"
    client = get_redis_client()
    log(f"Consultando Redis. key = {redis_key}")
    content = client.get(redis_key)
    if content is None:
        return content
    log(f"content = {content}")
    return convert_timezone(datetime.fromisoformat(content["last_run_timestamp"]))


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def get_ordem_quality_check_end_datetime(
    timestamp: datetime,
    start_datetime: Optional[datetime],
    end_datetime: Optional[str],
    partitions: Optional[list],
) -> Optional[datetime]:
    """
    Retorna o datetime final para fazer o teste de qualidade dos dados da ordem de pagamento

    Args:
        timestamp (datetime): Datetime do agendamento da run atual do Flow
        start_datetime (datetime): Datetime inicial da execução
        end_datetime (str): Parâmetro do flow para definir o valor manualmente
        partitions (list): Parâmetro do flow para definir as partições consultadas manualmente

    returns:
        Optional[datetime]: datetime agendado da execução do flow, valor do
        parâmetro end_datetime ou None se o parâmetro partitions não for nulo
    """
    if partitions is not None:
        return
    if end_datetime is not None:
        end_datetime = convert_timezone(datetime.fromisoformat(end_datetime))
    else:
        end_datetime = timestamp

    if start_datetime is not None and end_datetime < start_datetime:
        raise ValueError("datetime de início maior que datetime de fim!")

    log(f"end_datetime = {end_datetime}")
    return end_datetime


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
    nout=2,
)
def get_ordem_pagamento_modified_partitions(
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
    partitions: Optional[list],
) -> tuple[dict, Optional[str]]:
    """
    Consulta as partições para realizar o teste de qualidade de dados da ordem de pagamento

    Args:
        start_datetime (datetime): Datetime inicial da execução
        end_datetime (datetime): Datetime final da execução
        partitions (list): Parâmetro do flow para definir as partições consultadas manualmente

    Returns
        dict: Variável do DBT para filtrar os testes
        str: Nome do teste a ser executado, caso não existam partições modificadas
    """
    if partitions is None:
        format_str = "%Y-%m-%d %H:%M:%S"

        sql = """
        select
            parse_date('%Y%m%d', partition_id) as data_ordem
        from rj-smtr-dev.rafael__br_rj_riodejaneiro_bilhetagem.INFORMATION_SCHEMA.PARTITIONS
        where
            table_name = 'ordem_pagamento_consorcio_operador_dia'
            and partition_id != '__NULL__'
        """
        if start_datetime is not None:
            start_datetime = start_datetime.strftime(format_str)
            sql += f" and datetime(last_modified_time, 'America/Sao_Paulo') >= '{start_datetime}'"

        end_datetime = end_datetime.strftime(format_str)
        sql += f" and datetime(last_modified_time, 'America/Sao_Paulo') < '{end_datetime}'"

        log(f"executando query:\n{sql}")

        partitions = bd.read_sql(sql)["data_ordem"].to_list()
    else:
        partitions = [date.fromisoformat(p) for p in partitions]

    if len(partitions) > 0:
        test_name = None
    else:
        test_name = "dbt_expectations.expect_column_max_to_be_between__data_ordem__ordem_pagamento_consorcio_operador_dia"  # noqa

    log(f"partições = {partitions}")
    print(test_name)

    return {
        "partitions": ", ".join([f"date({p.year}, {p.month}, {p.day})" for p in partitions])
    }, test_name


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def set_redis_quality_check_datetime(
    env: str,
    dataset_id: str,
    table_id: str,
    end_datetime: datetime,
):
    """
    Define o valor no Redis para o end_datetime

    Args:
        env (str): prod ou dev
        dataset_id (str): dataset_id no BigQuery,
        table_id (str): table_id no BigQuery,
        end_datetime (datetime): Datetime final da execução
    """
    if end_datetime is None:
        return
    value = end_datetime.isoformat()
    redis_key = f"{env}.{dataset_id}.{table_id}.data_quality_check"
    log(f"salvando timestamp {value} na key: {redis_key}")
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    if not content:
        content = {"last_run_timestamp": value}
        redis_client.set(redis_key, content)
    else:
        if (
            convert_timezone(
                datetime.fromisoformat(
                    content["last_run_timestamp"],
                )
            )
            < end_datetime
        ):
            content["last_run_timestamp"] = value
            redis_client.set(redis_key, content)
