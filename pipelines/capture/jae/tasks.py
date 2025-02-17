# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Jaé"""
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone
from sqlalchemy import DATE, DATETIME, TIMESTAMP, create_engine, inspect

from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.utils import (
    create_billingpay_backup_filepath,
    get_redis_last_backup,
)
from pipelines.constants import constants as smtr_constants
from pipelines.utils.database import create_database_url, test_database_connection
from pipelines.utils.extractors.db import get_raw_db
from pipelines.utils.fs import create_partition, save_local_file
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.gcp.storage import Storage
from pipelines.utils.prefect import rename_current_flow_run
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_jae_general_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de tabelas da Jaé"""

    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    params = constants.JAE_TABLE_CAPTURE_PARAMS.value[source.table_id]

    start = (
        source.get_last_scheduled_timestamp(timestamp=timestamp)
        .astimezone(tz=timezone("UTC"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    end = timestamp.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    query = params["query"].format(start=start, end=end)
    database_name = params["database"]
    database = constants.JAE_DATABASE_SETTINGS.value[database_name]

    return partial(
        get_raw_db,
        query=query,
        engine=database["engine"],
        host=database["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=database_name,
    )


@task(nout=2)
def test_jae_databases_connections() -> tuple[bool, list[str]]:
    """
    Testa a conexão com os bancos de dados da Jaé

    Returns:
        bool: Se todas as conexões foram bem-sucedidas ou não
        list[str]: Lista com os nomes dos bancos de dados com falha de conexão
    """
    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    failed_connections = []
    for database_name, database in constants.JAE_DATABASE_SETTINGS.value.items():
        success, _ = test_database_connection(
            engine=database["engine"],
            host=database["host"],
            user=credentials["user"],
            password=credentials["password"],
            database=database_name,
        )
        if not success:
            failed_connections.append(database_name)

    return len(failed_connections) == 0, failed_connections


@task
def create_database_error_discord_message(failed_connections: list[str]) -> str:
    """
    Cria a mensagem para ser enviada no Discord caso haja
    problemas de conexão com os bancos da Jaé

    Args:
        failed_connections (list[str]): Lista com os nomes dos bancos de dados com falha de conexão
    Returns:
        str: Mensagem
    """
    message = "Falha de conexão com o(s) banco(s) de dados:\n"
    failed_connections = "\n".join(failed_connections)
    message += failed_connections
    return message + "\n"


# TASKS PARA O BACKUP DA BILLINGPAY #
@task
def rename_flow_run_backup_billingpay(database_name: str, timestamp: datetime):
    """
    Renomeia e execução do flow

    Args:
        database_name (str): Nome do banco de dados
        timestamp (datetime): Timestamp de referência da execução
    """
    rename_current_flow_run(name=f"{database_name}: {timestamp.isoformat()}")


@task
def get_jae_db_config(database_name: str) -> dict[str, str]:
    """
    Cria as configurações de conexão com o banco de dados

    Args:
        database_name (str): Nome do banco de dados

    Returns:
        dict[str, str]: Dicionário com os argumentos para a função create_database_url
    """
    secrets = get_secret(constants.JAE_SECRET_PATH.value)
    settings = constants.JAE_DATABASE_SETTINGS.value[database_name]
    return {
        "engine": settings["engine"],
        "host": settings["host"],
        "user": secrets["user"],
        "password": secrets["password"],
        "database": database_name,
    }


@task
def get_table_info(
    env: str,
    database_name: str,
    database_config: dict,
    timestamp: datetime,
) -> list[dict[str, str]]:
    """
    Busca as informações de todas as tabelas disponíveis em um banco de dados

    Args:
        env (str): prod ou dev
        database_name (str): Nome do banco de dados
        database_config (dict): Dicionário com os argumentos para a função create_database_url
        timestamp (datetime): Timestamp de referência da execução

    Returns:
        list[dict[str, str]]: Lista com dicionários contendo:
            - nome da tabela
            - tipo de carga incremental
            - caminho para salvar o arquivo
            - valor salvo no redis (se houver)
            - partição do arquivo
    """
    database_url = create_database_url(**database_config)
    engine = create_engine(database_url)
    inspector = inspect(engine)
    tables_config = constants.BACKUP_JAE_BILLING_PAY.value[database_name]
    partition = create_partition(timestamp=timestamp, partition_date_only=True)
    table_names = [
        t
        for t in inspector.get_table_names()
        if t not in tables_config.get("exclude", [])
        and isinstance(tables_config.get("filter", {}).get(t, []), list)
    ]
    result = [
        {
            "table_name": t,
            "incremental_type": None,
            "filepath": create_billingpay_backup_filepath(
                table_name=t,
                database_name=database_name,
                partition=partition,
                timestamp=timestamp,
            ),
            "partition": partition,
            "custom_select": tables_config.get("custom_select"),
        }
        for t in table_names
        if t not in tables_config["filter"]
    ]

    for table in [t for t in table_names if t in tables_config["filter"]]:

        filter_columns = tables_config["filter"][table]

        if filter_columns[0] == "count(*)":
            with engine.connect() as conn:
                current_count = pd.read_sql(f"select count(*) as ct from {table}", conn).to_dict()[
                    0
                ]["ct"]
                last_count = get_redis_last_backup(
                    env=env,
                    table_name=table,
                    database_name=database_name,
                    incremental_type="integer",
                )

                if current_count != last_count:
                    result.append(
                        {
                            "table_name": table,
                            "incremental_type": "count",
                            "filepath": create_billingpay_backup_filepath(
                                table_name=table,
                                database_name=database_name,
                                partition=partition,
                                timestamp=timestamp,
                            ),
                            "partition": partition,
                            "custom_select": tables_config.get("custom_select"),
                            "max_id": current_count,
                        }
                    )
            continue

        if len(filter_columns) > 1 or isinstance(
            [
                c["type"]
                for c in inspector.get_columns(table_name=table)
                if c["name"] in filter_columns
            ][0],
            (TIMESTAMP, DATE, DATETIME),
        ):
            incremental_type = "datetime"
        else:
            incremental_type = "integer"

        result.append(
            {
                "table_name": table,
                "incremental_type": incremental_type,
                "filepath": create_billingpay_backup_filepath(
                    table_name=table,
                    database_name=database_name,
                    partition=partition,
                    timestamp=timestamp,
                ),
                "last_capture": get_redis_last_backup(
                    env=env,
                    table_name=table,
                    database_name=database_name,
                    incremental_type=incremental_type,
                ),
                "partition": partition,
                "custom_select": tables_config.get("custom_select"),
            }
        )

    return result


@task(nout=2)
def get_non_filtered_tables(
    database_name: str,
    database_config: dict,
    table_info: list[dict[str, str]],
) -> tuple[bool, list[dict]]:
    """
    Busca tabelas com mais de 5000 linhas que não estejam com filtro configurado

    Args:
        database_name (str): Nome do banco de dados
        database_config (dict): Dicionário com os argumentos para a função create_database_url
        table_info (list[dict[str, str]]): Lista com as informações das tabelas

    Returns:
        bool: Se deve notificar o discord ou não
        list[dict]: Dicionário com as tabelas com mais de 5000 registros
    """
    tables_config = constants.BACKUP_JAE_BILLING_PAY.value[database_name]
    database_url = create_database_url(**database_config)
    engine = create_engine(database_url)
    result = []
    with engine.connect() as conn:
        for table in [
            t["table_name"] for t in table_info if t["table_name"] not in tables_config["filter"]
        ]:
            df = pd.read_sql(f"select count(*) as ct from {table}", conn)
            df["table"] = table
            result.append(df)
    df_final = pd.concat(result)
    tables = (
        df_final.loc[df_final["ct"] > 5000]
        .sort_values("ct", ascending=False)
        .to_dict(orient="records")
    )

    return len(tables) > 0, tables


@task
def create_non_filtered_discord_message(database_name: str, table_count: list[dict]) -> str:
    """
    Cria a mensagem para ser enviada no discord caso haja tabelas grandes sem filtro

    Args:
        database_name (str): Nome do banco de dados
        table_count (list[dict]): Dicionário com as tabelas e a contagem de registros

    Returns:
        str: Mensagem para ser enviada no discord
    """
    message = f"""
Database: {database_name}
As seguintes tabelas não possuem filtros:
"""
    message += "\n"
    message += "\n".join([f"{t['table']}: {t['ct']} registros" for t in table_count])
    return message


@task
def get_raw_backup_billingpay(
    table_info: list[dict[str, str]],
    database_config: dict,
    timestamp: datetime,
) -> list[dict[str, str]]:
    """
    Captura os dados das tabelas do banco informado

    Args:
        table_info (list[dict[str, str]]): Lista com as informações das tabelas
        database_config (dict): Dicionário com os argumentos para a função create_database_url
        timestamp (datetime): Timestamp de referência da execução

    Returns:
        list[dict[str, str]]: Lista com as informações das tabelas atualizada
    """
    timestamp_str = timestamp.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    new_table_info = []
    for table in table_info:
        table_name = table["table_name"]

        if table_info["custom_select"] is not None:
            table_name = f"({table_info['custom_select']})"

        sql = f"SELECT * FROM {table_name}"
        where = "1=1"
        if table["incremental_type"] == "datetime":
            last_capture_str = (
                table["last_capture"].astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
            )
            where = " OR ".join(
                [
                    f"({c} >= '{last_capture_str}' AND {c} < '{timestamp_str}')"
                    for c in constants.BACKUP_JAE_BILLING_PAY.value[database_config["database"]][
                        "filter"
                    ][table_name]
                ]
            )
        elif table["incremental_type"] == "integer":
            id_column = constants.BACKUP_JAE_BILLING_PAY.value[database_config["database"]][
                "filter"
            ][table_name][0]
            max_id = get_raw_db(
                f"select max({id_column}) as max_id FROM {table_name}",
                **database_config,
            )[0]["max_id"]
            where = f"{id_column} BETWEEN {table['last_capture']} AND {max_id}"
            table["max_id"] = max_id

        sql += f" WHERE {where}"

        data = get_raw_db(query=sql, **database_config)
        save_local_file(filepath=table["filepath"], filetype="json", data=data)
        new_table_info.append(table)
    return new_table_info


@task
def upload_backup_billingpay(env: str, table_info: dict[str, str], database_name: str) -> dict:
    """
    Sobe os dados do backup para o storage

    Args:
        env (str): prod ou dev
        table_info (list[dict[str, str]]): Dicionário com as informações da tabela
        database_name (str): Nome do banco de dados

    Retuns:
        dict: Dicionário com informações da tabela
    """
    Storage(env=env, dataset_id=database_name, table_id=table_info["table_name"],).upload_file(
        mode=constants.BACKUP_BILLING_PAY_FOLDER.value,
        filepath=table_info["filepath"],
        partition=table_info["partition"],
    )

    return table_info


@task
def set_redis_backup_billingpay(
    env: str,
    table_info: dict[str, str],
    database_name: str,
    timestamp: datetime,
):
    """
    Atualiza o Redis com os novos dados capturados

    Args:
        env (str): prod ou dev
        table_info (list[dict[str, str]]): Dicionário com as informações da tabela
        database_name (str): Nome do banco de dados
        timestamp (datetime): Timestamp de referência da execução
    """
    if table_info["incremental_type"] is None:
        return
    redis_key = f"{env}.backup_jae_billingpay.{database_name}.{table_info['table_name']}"
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    if table_info["incremental_type"] == "datetime":
        save_value = timestamp.strftime(smtr_constants.MATERIALIZATION_LAST_RUN_PATTERN.value)
    else:
        save_value = table_info["max_id"]

    if not content:
        log(f"Saving value: {save_value} on key {redis_key}")
        content = {constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY.value: save_value}
        redis_client.set(redis_key, content)
    elif (
        content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY.value] < save_value
        or table_info["incremental_type"] == "count"
    ):
        log(f"Saving value: {save_value} on key {redis_key}")
        content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY.value] = save_value
        redis_client.set(redis_key, content)
    else:
        log(f"[{redis_key}] {save_value} é menor que o valor salvo no Redis")
