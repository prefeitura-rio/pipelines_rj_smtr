# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta
from typing import Union

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone
from sqlalchemy import create_engine

from pipelines.capture.jae.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.database import create_database_url
from pipelines.utils.extractors.db import get_raw_db
from pipelines.utils.fs import get_data_folder_path, save_local_file
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import convert_timezone


def create_billingpay_backup_filepath(
    table_name: str,
    database_name: str,
    partition: str,
    timestamp: datetime,
) -> str:
    """
    Cria o caminho para salvar os dados de backup da BillingPay

    Args:
        table_name (str): Nome da tabela
        database_name (str): Nome do banco de dados
        partition (str): Partição no formato Hive
        timestamp (datetime): Timestamp de referência da execução

    Returns:
        str: Caminho para o arquivo
    """
    return os.path.join(
        get_data_folder_path(),
        constants.BACKUP_BILLING_PAY_FOLDER.value,
        database_name,
        table_name,
        partition,
        f"{timestamp.strftime(smtr_constants.FILENAME_PATTERN.value)}_{{n}}.json",
    )


def get_redis_last_backup(
    env: str,
    table_name: str,
    database_name: str,
    incremental_type: str,
) -> Union[int, datetime]:
    """
    Consulta no Redis o último valor capturado de uma tabela

    Args:
        env (str): prod ou dev
        table_name (str): Nome da tabela
        database_name (str): Nome do banco de dados
        database_config (dict): Dicionário com os argumentos para a função create_database_url
        incremental_type (str): Tipo de carga incremental (datetime ou integer)

    Returns:
        Union[int, datetime]: Último valor capturado
    """
    redis_key = f"{env}.backup_jae_billingpay.{database_name}.{table_name}"
    log(f"Consultando Redis: {redis_key}")
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    log(f"content = {content}")
    if incremental_type == "datetime":
        last_datetime = (
            datetime(1900, 1, 1, 0, 0, 0)
            if content is None
            else datetime.strptime(
                content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY.value],
                smtr_constants.MATERIALIZATION_LAST_RUN_PATTERN.value,
            )
        )

        return convert_timezone(timestamp=last_datetime)
    if incremental_type == "integer":
        last_id = (
            0
            if content is None
            else int(content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY.value])
        )
        return last_id

    raise ValueError(f"Tipo {incremental_type} não encontrado.")


def get_table_data_backup_billingpay(
    query: str,
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
    filepath: str,
    page_size: int,
) -> list[str]:
    """
    Captura dados de um Banco de Dados SQL fazendo paginação

    Args:
        query (str): o SELECT para ser executado
        engine (str): O banco de dados (postgresql ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)
        filepath (str): Modelo para criar o caminho para salvar os dados
    Returns:
        list[str]: Lista de arquivos salvos
    """
    offset = 0
    base_query = f"{query} LIMIT {page_size}"
    query = f"{base_query} OFFSET 0"
    page_data_len = page_size
    current_page = 0
    filepaths = []
    while page_data_len == page_size:
        data = get_raw_db(
            query=query,
            engine=engine,
            host=host,
            user=user,
            password=password,
            database=database,
        )
        save_filepath = filepath.format(n=current_page)
        save_local_file(filepath=save_filepath, filetype="json", data=data)
        filepaths.append(save_filepath)
        page_data_len = len(data)
        log(
            f"""
            Page size: {page_size}
            Current page: {current_page}
            Current page returned {page_data_len} rows"""
        )
        current_page += 1
        offset = current_page * page_size
        query = f"{base_query} OFFSET {offset}"

    return filepaths


def get_jae_timestamp_captura_count(
    table_id: str,
    timestamp_column: str,
    timestamp_captura_start: datetime,
    timestamp_captura_end: datetime,
) -> pd.DataFrame:
    table_capture_params = constants.JAE_TABLE_CAPTURE_PARAMS.value[table_id]
    database = table_capture_params["database"]
    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    database_settings = constants.JAE_DATABASE_SETTINGS.value[database]
    url = create_database_url(
        engine=database_settings["engine"],
        host=database_settings["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=database,
    )
    connection = create_engine(url)
    capture_delay = table_capture_params.get("capture_delay_minutes", 0)

    base_query_jae = f"""
        WITH timestamps_captura AS (
            SELECT generate_series(
                timestamp '{{timestamp_captura_start}}',
                timestamp '{{timestamp_captura_end}}',
                interval '1 minute'
            ) AS timestamp_captura
        ),
        dados_jae AS (
            {table_capture_params["query"]}
        )
        contagens AS (
            SELECT
                date_trunc(
                    'minute', {timestamp_column}
                )
                - INTERVAL '{{delay}} minutes'
                - INTERVAL '1 minutes' AS timestamp_captura,
                COUNT(id) AS total_jae
            FROM
                dados_jae
            GROUP BY
                minuto
        )
        SELECT
            tc.timestamp_captura,
            COALESCE(c.total_jae, 0) AS total_jae
        FROM
            timestamps_captura tc
        LEFT JOIN
            contagens c USING(timestamp_captura)
        ;
    """

    jae_start_ts = timestamp_captura_start
    jae_result = []
    """
    2025-07-22 00:00:00
    2025-04 24 07:00:00

    2025-07-22 00:01:00
    2025-07-23 00:00:00


    """
    while jae_start_ts < timestamp_captura_end:
        jae_end_ts = min(jae_start_ts + timedelta(days=1), timestamp_captura_end)

        jae_start_ts_utc = jae_start_ts.astimezone(tz=timezone("UTC"))
        jae_end_ts_utc = jae_end_ts.astimezone(tz=timezone("UTC"))

        df_count_jae = pd.read_sql(
            sql=base_query_jae.format(
                timestamp_captura_start=jae_start_ts_utc.strftime("%Y-%m-%d %H:%M:%S"),
                timestamp_captura_end=jae_end_ts_utc.strftime("%Y-%m-%d %H:%M:%S"),
                start=jae_start_ts_utc.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                end=jae_end_ts_utc.replace(hour=23, minute=59, second=59, microsecond=59).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                delay=capture_delay,
            ),
            con=connection,
        )
        df_count_jae["timestamp_captura"] = (
            pd.to_datetime(df_count_jae["timestamp_captura"])
            .dt.tz_localize("UTC")
            .dt.tz_convert("America/Sao_Paulo")
        )

        jae_result

        jae_start_ts = jae_end_ts + timedelta(minutes=1)
