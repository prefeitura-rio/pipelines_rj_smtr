# -*- coding: utf-8 -*-
"""Tasks de captura dos dados da Jaé"""
from datetime import datetime, timedelta
from functools import partial
from typing import Optional

import pandas as pd
import pandas_gbq
import prefect
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone
from sqlalchemy import DATE, DATETIME, TIMESTAMP, create_engine, inspect

from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.utils import (
    create_billingpay_backup_filepath,
    get_capture_delay_minutes,
    get_jae_timestamp_captura_count,
    get_redis_last_backup,
    get_table_data_backup_billingpay,
    save_capture_check_results,
)
from pipelines.constants import constants as smtr_constants
from pipelines.utils.database import (
    create_database_url,
    list_accessible_tables,
    test_database_connection,
)
from pipelines.utils.extractors.db import get_raw_db, get_raw_db_paginated
from pipelines.utils.fs import create_partition
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.gcp.storage import Storage
from pipelines.utils.prefect import rename_current_flow_run
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import convert_timezone


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_jae_general_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de tabelas da Jaé"""

    if source.table_id == constants.GPS_VALIDADOR_TABLE_ID.value and timestamp < convert_timezone(
        datetime(2025, 3, 26, 15, 31, 0)
    ):
        raise ValueError(
            """A recaptura de dados anteriores deve ser feita manualmente.
            A coluna de captura foi alterada de ID para data_tracking"""
        )

    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    params = constants.JAE_TABLE_CAPTURE_PARAMS.value[source.table_id]

    start = source.get_last_scheduled_timestamp(timestamp=timestamp).astimezone(tz=timezone("UTC"))
    end = timestamp.astimezone(tz=timezone("UTC"))

    if source.table_id == constants.TRANSACAO_ORDEM_TABLE_ID.value:
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=6, minute=0, second=0)

    start = start.strftime("%Y-%m-%d %H:%M:%S")
    end = end.strftime("%Y-%m-%d %H:%M:%S")
    capture_delay_minutes = params.get("capture_delay_minutes", {"0": 0})

    delay = get_capture_delay_minutes(
        capture_delay_minutes=capture_delay_minutes, timestamp=timestamp
    )

    query = params["query"].format(
        start=start,
        end=end,
        delay=delay,
    )
    database_name = params["database"]
    database = constants.JAE_DATABASE_SETTINGS.value[database_name]
    general_func_arguments = {
        "query": query,
        "engine": database["engine"],
        "host": database["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": database_name,
        "max_retries": 3,
    }
    if source.file_chunk_size is not None:
        return partial(
            get_raw_db_paginated, page_size=source.file_chunk_size, **general_func_arguments
        )
    return partial(get_raw_db, **general_func_arguments)


@task
def create_ressarcimento_db_extractor(source: SourceTable, timestamp: datetime):
    """Cria a extração de tabelas do ressarcimento_db da Jaé"""
    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    params = constants.JAE_TABLE_CAPTURE_PARAMS.value[source.table_id]

    end = timestamp.astimezone(tz=timezone("UTC"))
    start = (end - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    end = end.strftime("%Y-%m-%d %H:%M:%S")

    capture_delay_minutes = params.get("capture_delay_minutes", {"0": 0})

    delay = get_capture_delay_minutes(
        capture_delay_minutes=capture_delay_minutes, timestamp=timestamp
    )

    query = params["query"].format(
        start=start,
        end=end,
        delay=delay,
    )

    database_name = params["database"]
    database = constants.JAE_DATABASE_SETTINGS.value[database_name]
    general_func_arguments = {
        "query": query,
        "engine": database["engine"],
        "host": database["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": database_name,
        "max_retries": 3,
    }
    if source.file_chunk_size is not None:
        return partial(
            get_raw_db_paginated, page_size=source.file_chunk_size, **general_func_arguments
        )
    return partial(get_raw_db, **general_func_arguments)


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
    table_id: str = None,
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
    if table_id is not None:
        table_names = [table_id]
    else:
        table_names = [
            t
            for t in list_accessible_tables(engine=engine)
            if t not in tables_config.get("exclude", [])
            and isinstance(tables_config.get("filter", {}).get(t, []), list)
        ]

    custom_select = tables_config.get("custom_select", {})
    filtered_tables = tables_config.get("filter", [])
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
            "custom_select": custom_select.get(t),
        }
        for t in table_names
        if t not in filtered_tables
    ]

    for table in [t for t in table_names if t in filtered_tables]:

        filter_columns = filtered_tables[table]

        if filter_columns[0] == "count(*)":
            with engine.connect() as conn:
                current_count = pd.read_sql(f"select count(*) as ct from {table}", conn).to_dict(
                    orient="records"
                )[0]["ct"]
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
                            "custom_select": custom_select.get(table),
                            "redis_save_value": current_count,
                        }
                    )
            continue

        if (
            len(filter_columns) > 1
            or table in custom_select.keys()
            or isinstance(
                [
                    c["type"]
                    for c in inspector.get_columns(table_name=table)
                    if c["name"] in filter_columns
                ][0],
                (TIMESTAMP, DATE, DATETIME),
            )
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
                "custom_select": custom_select.get(table),
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
    no_filter_tables = [
        t["table_name"]
        for t in table_info
        if t["table_name"] not in tables_config.get("filter", [])
    ]
    if len(no_filter_tables) == 0:
        return False, []
    database_url = create_database_url(**database_config)
    engine = create_engine(database_url)
    result = []
    with engine.connect() as conn:
        for table in no_filter_tables:
            log(table)
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

    new_table_info = []
    for table in table_info:
        table_name = table["table_name"]
        if table["custom_select"] is not None:
            sql = table["custom_select"]
        else:
            sql = f"SELECT * FROM {table_name}"

        if "{filter}" not in sql:
            sql += " WHERE {filter}"

        where = "1=1"
        if table["incremental_type"] == "datetime":
            timestamp_str = (
                table.get(
                    "last_value",
                    timestamp,
                )
                .astimezone(tz=timezone("UTC"))
                .strftime("%Y-%m-%d %H:%M:%S")
            )
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
            table["redis_save_value"] = max_id
        sql = sql.format(filter=where)

        filepath = get_table_data_backup_billingpay(
            query=sql,
            filepath=table["filepath"],
            page_size=constants.BACKUP_JAE_BILLING_PAY.value[database_config["database"]]
            .get("page_size", {})
            .get(table_name, 200_000),
            **database_config,
        )
        table["filepath"] = filepath
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
    for filepath in table_info["filepath"]:
        Storage(env=env, dataset_id=database_name, table_id=table_info["table_name"]).upload_file(
            mode=constants.BACKUP_BILLING_PAY_FOLDER.value,
            filepath=filepath,
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
        save_value = table_info["redis_save_value"]

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


@task
def get_timestamps_historic_table(
    env: str,
    database_name: str,
    table_info: list[dict],
) -> list[dict]:
    """
    Consulta no Redis os ultimos timestamps capturados das tabelas

    Args:
        env (str): prod ou dev
        database_name (str): Nome do banco de dados
        table_info (list[dict[str, str]]): Dicionário com as informações da tabela

    Returns:
        list[dict]: table_info atualizado
    """
    redis_client = get_redis_client()
    capture_tables = constants.BACKUP_JAE_BILLING_PAY_HISTORIC.value[database_name]
    for table in table_info:
        redis_key = (
            f"{env}.backup_jae_billingpay_historic_capture.{database_name}.{table['table_name']}"
        )
        content = redis_client.get(redis_key)

        if content is None:
            table["timestamp"] = convert_timezone(capture_tables[table["table_name"]]["start"])
        else:
            table["timestamp"] = convert_timezone(datetime.fromisoformat(content))

        table["partition"] = create_partition(
            timestamp=table["timestamp"], partition_date_only=True
        )
        table["filepath"] = create_billingpay_backup_filepath(
            table_name=table["table_name"],
            database_name=database_name,
            partition=table["partition"],
            timestamp=table["timestamp"],
        )

        table["last_capture"] = table["timestamp"]

    return table_info


@task
def get_end_value_historic_table(
    table_info: list[dict], database_name: str, database_config: dict
) -> list[dict]:
    """
    Atualiza as informações da tabela com o timestamp e o ultimo valor para capturar o histórico
    das tabelas grandes

    Args:
        table_info (list[dict[str, str]]): Dicionário com as informações da tabela
        database_name (str): Nome do banco de dados

    Returns:
        list[dict]: table_info atualizado
    """
    result = []
    for table in table_info:
        table_name = table["table_name"]
        table_end = convert_timezone(
            constants.BACKUP_JAE_BILLING_PAY_HISTORIC.value[database_name][table_name]["end"]
        )
        if table["timestamp"] == table_end:
            continue
        filter_columns = constants.BACKUP_JAE_BILLING_PAY.value[database_config["database"]][
            "filter"
        ][table_name]

        if len(filter_columns) == 1:
            if table["custom_select"] is not None:
                sql = table["custom_select"]
            else:
                sql = f"SELECT * FROM {table['table_name']}"

            if "{filter}" not in sql:
                sql += " WHERE {filter}"

            last_capture_str = (
                table["last_capture"].astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
            )

            where = f"{filter_columns[0]} >= '{last_capture_str}'"

            sql = sql.format(filter=where)
            max_dt = get_raw_db(
                f"select max({filter_columns[0]}) as max_dt FROM ({sql} limit 2000000) a",
                **database_config,
            )[0]["max_dt"]
            max_dt = min(
                convert_timezone(max_dt.tz_localize("UTC").to_pydatetime()),
                table["timestamp"] + timedelta(days=1),
            )
        else:
            max_dt = table["timestamp"] + timedelta(days=1)

        table["last_value"] = min(max_dt, table_end)

        result.append(table)
    return result


@task
def set_redis_historic_table(
    env: str,
    table_info: dict,
    database_name: str,
):
    """
    Atualiza o Redis com os novos dados capturados

    Args:
        env (str): prod ou dev
        table_info (list[dict[str, str]]): Dicionário com as informações da tabela
        database_name (str): Nome do banco de dados
    """
    redis_key = (
        f"{env}.backup_jae_billingpay_historic_capture.{database_name}.{table_info['table_name']}"
    )
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    save_value = table_info["last_value"].isoformat()
    if not content:
        log(f"Saving value: {save_value} on key {redis_key}")
        redis_client.set(redis_key, save_value)
    elif save_value > content:
        log(f"Saving value: {save_value} on key {redis_key}")
        redis_client.set(redis_key, save_value)
    else:
        log(f"[{redis_key}] {save_value} é menor que o valor salvo no Redis")


# TASKS PARA VERIFICAÇÃO DE GAPS NA CAPTURA #


@task
def rename_flow_run_jae_capture_check(
    timestamp_captura_start: datetime, timestamp_captura_end: datetime
):
    """
    Renomeia a execução do flow de checagem da captura da Jaé

    Args:
        timestamp_captura_start (datetime): Data e hora inicial da janela de verificação.
        timestamp_captura_end (datetime): Data e hora final da janela de verificação.
    """

    start = timestamp_captura_start.isoformat()
    end = timestamp_captura_end.isoformat()
    rename_current_flow_run(name=f"verificacao captura: from {start} to {end}")


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
    nout=2,
)
def jae_capture_check_get_ts_range(
    timestamp: datetime,
    retroactive_days: int,
    timestamp_captura_start: Optional[str],
    timestamp_captura_end: Optional[str],
) -> tuple[datetime, datetime]:
    """
    Calcula o intervalo de para checagem da captura dos dados da Jaé.

    Args:
        timestamp (datetime): Data e hora de execução do flow
        retroactive_days (int): Número de dias a subtrair de `timestamp` para definir
            o início do intervalo
        timestamp_captura_start (Optional[str]): Parâmetro do flow para definição
            de timestamp inicial de forma manual
        timestamp_captura_end (Optional[str]): Parâmetro do flow para definição
            de timestamp final de forma manual

    Returns:
        tuple[datetime, datetime]: Intervalo com:
            - start (datetime): Início do intervalo
            - end (datetime): Fim do intervalo
    """
    if timestamp_captura_start is not None:
        start = datetime.fromisoformat(timestamp_captura_start)
    else:
        start = (timestamp - timedelta(days=retroactive_days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    start = convert_timezone(timestamp=start)

    if timestamp_captura_end is not None:
        end = datetime.fromisoformat(timestamp_captura_end)
    else:
        end = start.replace(hour=23, minute=59, second=59, microsecond=0)

    end = convert_timezone(timestamp=end)

    return start, end


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def get_capture_gaps(
    env: str,
    table_id: str,
    timestamp_captura_start: datetime,
    timestamp_captura_end: datetime,
) -> list[str]:
    """
    Identifica timestamps com divergência entre os dados presentes
    na base da Jaé e os dados capturados no datalake.

    Args:
        env (str): prod ou dev
        table_id (str): Nome da tabela no BigQuery
        timestamp_captura_start (datetime): Início do intervalo de verificação
        timestamp_captura_end (datetime): Fim do intervalo de verificação

    Returns:
        list[str]: Lista de strings no formato `%Y-%m-%d %H:%M:%S` representando os timestamps
        com divergência de contagem entre JAE e datalake
    """
    params = constants.CHECK_CAPTURE_PARAMS.value[table_id]
    timestamp_column = params["timestamp_column"]
    source = params["source"]
    df_jae = get_jae_timestamp_captura_count(
        source=source,
        timestamp_column=timestamp_column,
        timestamp_captura_start=timestamp_captura_start,
        timestamp_captura_end=timestamp_captura_end,
        final_timestamp_exclusive=params["final_timestamp_exclusive"],
    )

    primary_keys = params.get("primary_keys")
    if primary_keys is None:
        primary_keys = "1"
    elif len(primary_keys) == 1:
        primary_keys = f"DISTINCT {primary_keys[0]}"
    else:
        primary_keys = f"DISTINCT TO_JSON_STRING(STRUCT({', '.join(primary_keys)}))"

    query_datalake = f"""
    WITH contagens AS (
        SELECT
            timestamp_captura,
            COUNT({primary_keys}) AS total_datalake
        FROM
            {params['datalake_table']}
        WHERE
            DATA BETWEEN '{timestamp_captura_start.date().isoformat()}'
            AND '{timestamp_captura_end.date().isoformat()}'
            AND timestamp_captura BETWEEN '{timestamp_captura_start.strftime("%Y-%m-%d %H:%M:%S")}'
            AND '{timestamp_captura_end.strftime("%Y-%m-%d %H:%M:%S")}'
        GROUP BY
            1
    ),
    timestamps_captura AS (
        SELECT
            DATETIME(timestamp_captura) AS timestamp_captura
        FROM
            UNNEST(
                GENERATE_TIMESTAMP_ARRAY(
                    '{timestamp_captura_start.strftime("%Y-%m-%d %H:%M:%S")}',
                    '{timestamp_captura_end.strftime("%Y-%m-%d %H:%M:%S")}',
                    INTERVAL 1 minute
                )
            ) AS timestamp_captura
    )
    SELECT
        timestamp_captura,
        COALESCE(total_datalake, 0) AS total_datalake
    FROM
        timestamps_captura
    LEFT JOIN
        contagens
    USING
        (timestamp_captura)
    """

    log(f"Executando query\n{query_datalake}")

    df_datalake = pandas_gbq.read_gbq(
        query_datalake,
        project_id=smtr_constants.PROJECT_NAME.value[env],
    )

    df_datalake["timestamp_captura"] = df_datalake["timestamp_captura"].dt.tz_localize(
        smtr_constants.TIMEZONE.value
    )

    df_merge = df_jae.merge(df_datalake, how="left", on="timestamp_captura")
    df_merge["table_id"] = table_id
    df_merge["total_datalake"] = df_merge["total_datalake"].astype(int)
    df_merge["total_jae"] = df_merge["total_jae"].astype(int)
    df_merge["indicador_captura_correta"] = df_merge["total_datalake"] == df_merge["total_jae"]

    timestamps = (
        df_merge.loc[~df_merge["indicador_captura_correta"]]
        .sort_values(by=["timestamp_captura"])["timestamp_captura"]
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .tolist()
    )

    save_capture_check_results(env=env, results=df_merge)

    if len(timestamps) > 0:
        ts_log = [f'"{t}",' for t in timestamps]
        log(
            "[{table_id}] Os seguintes timestamps estão divergentes:\n{timestamps_str}".format(
                table_id=table_id, timestamps_str="\n".join(ts_log)
            )
        )
    else:
        log(f"[{table_id}] Todos os dados foram capturados com sucesso!")

    return timestamps


@task
def create_capture_check_discord_message(
    table_id: str,
    timestamps: list[dict],
    timestamp_captura_start: datetime,
    timestamp_captura_end: datetime,
) -> str:
    """
    Cria a mensagem para notificação no Discord com o resultado da verificação de captura de dados

    Args:
        table_id (str): Nome da tabela no BigQuery
        timestamps (list[dict]): Lista de timestamps com falhas na captura
        timestamp_captura_start (datetime): Início do intervalo analisado
        timestamp_captura_end (datetime): Fim do intervalo analisado

    Returns:
        str: Mensagem para ser enviada no Discord
    """
    timestamps_len = len(timestamps)
    message = f"""
Tabela: {table_id}
De {timestamp_captura_start.isoformat()} até {timestamp_captura_end.isoformat()}
Foram encontradas {timestamps_len} timestamps com dados faltantes
"""
    if timestamps_len > 0:
        mentions_tag = (
            f" - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS.value['dados_smtr']['user_id']}>"
        )
        message = f":red_circle: {message}"
        message += (
            "\n"
            + f"https://pipelines.dados.rio/flow-run/{prefect.context.flow_run_id}"
            + "\n"
            + mentions_tag
        )
    else:
        message = f":green_circle: {message}"
    return message
