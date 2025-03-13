# -*- coding: utf-8 -*-
"""Tasks de tratamento dos dados financeiros"""
from datetime import date, datetime, timedelta

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
    start_datetime: str,
    partitions: list,
) -> datetime:
    """ """
    if partitions is not None:
        return
    if start_datetime is not None:
        return convert_timezone(datetime.fromisoformat(start_datetime))

    redis_key = f"{env}.{dataset_id}.{table_id}.data_quality_check"
    client = get_redis_client(host="localhost")
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
    start_datetime: datetime,
    end_datetime: str,
    partitions: list,
) -> datetime:
    """"""
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
    start_datetime: datetime,
    end_datetime: datetime,
    partitions: list,
) -> str:

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
    if end_datetime is None:
        return
    value = end_datetime.isoformat()
    redis_key = f"{env}.{dataset_id}.{table_id}.data_quality_check"
    log(f"salvando timestamp {value} na key: {redis_key}")
    redis_client = get_redis_client(host="localhost")
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
