# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta
from typing import Union

import pandas as pd
import pandas_gbq
from croniter import croniter
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client
from pytz import timezone
from sqlalchemy import create_engine

from pipelines.capture.jae.constants import JAE_SOURCE_NAME, constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.database import create_database_url
from pipelines.utils.extractors.db import get_raw_db
from pipelines.utils.fs import get_data_folder_path, save_local_file
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import convert_timezone


def get_capture_delay_minutes(capture_delay_minutes: dict[str, int], timestamp: datetime) -> int:
    """
    Retorna a quantidade de minutos a ser subtraído do inicio e fim do filtro de captura
    para um determinado timestamp

    Args:
        capture_delay_minutes (dict[str, int]):
            Dicionário que mapeia timestamps em formato string ISO
            (`"%Y-%m-%d %H:%M:%S"`) para valores de delay em minutos.
            A chave `"0"` representa o primeiro delay
        timestamp (datetime):
            Timestamp de captura para o qual se deseja calcular o atraso.

    Returns:
        int: O atraso em minutos correspondente ao `timestamp`.

    Example:
        >>> capture_delay_minutes = {
        ...     "0": 5,
        ...     "2025-09-25 12:00:00": 10,
        ...     "2025-09-26 09:00:00": 15,
        ... }
        >>> get_capture_delay_minutes(capture_delay_minutes, datetime(2025, 9, 26, 10, 0))
        15
    """
    delay_timestamps = (
        convert_timezone(timestamp=datetime.fromisoformat(a))
        for a in capture_delay_minutes.keys()
        if a != "0"
    )
    delay = capture_delay_minutes["0"]
    for t in delay_timestamps:
        if timestamp >= t:
            delay = capture_delay_minutes[t.strftime("%Y-%m-%d %H:%M:%S")]

    return int(delay)


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


def get_jae_timestamp_captura_count_query(
    engine: str,
    delay_query: str,
    capture_interval_minutes: int,
    capture_query: str,
    timestamp_column: str,
    final_timestamp_exclusive: bool,
) -> str:
    """
    Gera uma query SQL para contar os dados da Jaé adaptada para PostgreSQL ou MySQL.

    Args:
        engine (str): Nome do banco de dados ('postgresql' ou 'mysql').
        delay_query (str): Expressão SQL usada para calcular o delay da captura.
        capture_interval_minutes (int): Intervalo de captura em minutos.
        capture_query (str): Query base que retorna os dados de captura.
        timestamp_column (str): Nome da coluna de timestamp da tabela.

    Returns:
        str: Query SQL completa para contar registros agrupados por timestamp_captura.
    """

    if engine == "postgresql":
        if final_timestamp_exclusive:
            count_query = f"""
                SELECT
                    TO_TIMESTAMP(
                        FLOOR(
                            EXTRACT(
                                EPOCH FROM {timestamp_column}) / ({capture_interval_minutes} * 60
                            )
                        ) * ({capture_interval_minutes} * 60)
                    )  AT TIME ZONE 'UTC' AS datetime_truncado,
                    COUNT(1) AS total_jae
                FROM
                    dados_jae
                GROUP BY
                    1

            """
        else:
            count_query = f"""
                SELECT
                    datetime_truncado,
                    COUNT(1) AS total_jae
                FROM
                (
                    SELECT
                        *,
                        TO_TIMESTAMP(
                            FLOOR(
                                EXTRACT(
                                    EPOCH FROM {timestamp_column}
                                )
                                / ({capture_interval_minutes} * 60)
                            ) * ({capture_interval_minutes} * 60)
                        )  AT TIME ZONE 'UTC' AS datetime_truncado
                    FROM dados_jae

                    UNION ALL

                    SELECT
                        *,
                        TO_TIMESTAMP(
                            FLOOR(
                                EXTRACT(
                                    EPOCH FROM {timestamp_column} - interval '1 second'
                                ) / ({capture_interval_minutes} * 60
                                )
                            ) * ({capture_interval_minutes} * 60)
                        )  AT TIME ZONE 'UTC' AS datetime_truncado
                    FROM
                        dados_jae
                    WHERE
                        TO_TIMESTAMP(
                            FLOOR(
                                EXTRACT(
                                    EPOCH FROM {timestamp_column})
                                    / ({capture_interval_minutes} * 60
                                )
                            ) * ({capture_interval_minutes} * 60)
                        )  AT TIME ZONE 'UTC' = {timestamp_column}
                )
                GROUP BY
                        1
            """
        return f"""
            WITH timestamps_captura AS (
                SELECT timestamp_captura, {delay_query} AS delay
                FROM (SELECT generate_series(
                    timestamp '{{timestamp_captura_start}}',
                    timestamp '{{timestamp_captura_end}}',
                    interval '{capture_interval_minutes} minute'
                ) AS timestamp_captura)
            ),
            dados_jae AS (
                {capture_query}
            ),
            contagens AS (
                {count_query}
            )
            SELECT
                tc.timestamp_captura,
                COALESCE(c.total_jae, 0) AS total_jae
            FROM
                timestamps_captura tc
            LEFT JOIN
                contagens c
            ON
                tc.timestamp_captura = c.datetime_truncado
                + (tc.delay + {capture_interval_minutes} || ' minutes')::interval
        """

    elif engine == "mysql":

        if final_timestamp_exclusive:
            join_condition = f"""
                d.{timestamp_column} >= tc.timestamp_inicial AND
                d.{timestamp_column} < tc.timestamp_final
            """
        else:
            join_condition = f"""
                d.{timestamp_column} BETWEEN tc.timestamp_inicial
                    AND tc.timestamp_final
            """

            return f"""
                WITH RECURSIVE timestamps_captura AS (
                    SELECT
                        TIMESTAMP('{{timestamp_captura_start}}') AS timestamp_captura,
                        DATE_SUB(
                            TIMESTAMP('{{timestamp_captura_start}}'),
                            INTERVAL ({delay_query} + {capture_interval_minutes}) MINUTE
                        ) AS timestamp_inicial,
                        DATE_SUB(
                            TIMESTAMP('{{timestamp_captura_start}}'),
                            INTERVAL ({delay_query}) MINUTE
                        ) AS timestamp_final
                    UNION ALL
                    SELECT
                        timestamp_captura + INTERVAL {capture_interval_minutes} MINUTE,
                        DATE_SUB(
                            timestamp_captura  + INTERVAL {capture_interval_minutes} MINUTE,
                            INTERVAL ({delay_query} + {capture_interval_minutes}) MINUTE
                        ) AS timestamp_inicial,
                        DATE_SUB(
                            timestamp_captura  + INTERVAL {capture_interval_minutes} MINUTE,
                            INTERVAL ({delay_query}) MINUTE
                        ) AS timestamp_final
                    FROM timestamps_captura
                    WHERE
                        timestamp_captura
                        + INTERVAL {capture_interval_minutes} MINUTE
                        <= TIMESTAMP('{{timestamp_captura_end}}')
                ),
                dados_jae AS (
                    {capture_query}
                ),
                jae_timestamp_captura AS (
                    SELECT
                        tc.timestamp_captura,
                        d.{timestamp_column} as col
                    FROM
                        timestamps_captura tc
                    LEFT JOIN
                        dados_jae d
                    ON {join_condition}
                )
                SELECT
                    timestamp_captura,
                    count(col) AS total_jae
                FROM
                    jae_timestamp_captura
                GROUP BY
                    timestamp_captura
            """

    else:
        raise NotImplementedError(f"Engine {engine} não implementada")


def get_capture_interval_minutes(source: SourceTable) -> int:
    """
    Retorna o intervalo de captura em minutos calculado a partir do cron do SourceTable.

    Args:
        source (SourceTable): Objeto que contém a expressão cron na propriedade `schedule_cron`.

    Returns:
        int: Intervalo entre capturas, em minutos.
    """
    cron_expr = source.schedule_cron
    base_time = datetime.now()
    iterador = croniter(cron_expr, base_time)
    next_time = iterador.get_next(datetime)
    prev_time = iterador.get_prev(datetime)

    return (next_time - prev_time).total_seconds() / 60


def get_jae_timestamp_captura_count(
    source: SourceTable,
    timestamp_column: str,
    timestamp_captura_start: datetime,
    timestamp_captura_end: datetime,
    final_timestamp_exclusive: bool,
) -> pd.DataFrame:
    """
    Retorna a contagem de registros por timestamp_captura de uma tabela da Jaé.

    Args:
        source (SourceTable): Objeto contendo informações da tabela
        timestamp_column (str): Nome da coluna de timestamp que os dados capturados são filtrados
        timestamp_captura_start (datetime): Data e hora inicial da janela de captura
        timestamp_captura_end (datetime): Data e hora final da janela de captura

    Returns:
        pd.DataFrame: DataFrame com duas colunas:
            - `timestamp_captura` (datetime): Coluna timestamp_captura correspondente.
            - `total_jae` (int): Contagem de registros na base da Jaé.
    """
    table_capture_params = constants.JAE_TABLE_CAPTURE_PARAMS.value[source.table_id]
    database = table_capture_params["database"]
    credentials = get_secret(constants.JAE_SECRET_PATH.value)
    database_settings = constants.JAE_DATABASE_SETTINGS.value[database]
    engine = database_settings["engine"]
    url = create_database_url(
        engine=engine,
        host=database_settings["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=database,
    )
    connection = create_engine(url)
    capture_delay_minutes = table_capture_params.get("capture_delay_minutes", {"0": 0})
    capture_delay_timestamps = [a for a in capture_delay_minutes.keys() if a != "0"]

    if len(capture_delay_timestamps) == 0:
        delay_query = f"{capture_delay_minutes['0']}"
    else:
        delay_query = "CASE\n"
        for t in [a for a in capture_delay_timestamps if a != "0"]:
            tc = (
                convert_timezone(timestamp=datetime.fromisoformat(t))
                .astimezone(tz=timezone("UTC"))
                .strftime("%Y-%m-%d %H:%M:%S")
            )
            delay_query += f"WHEN timestamp_captura >= '{tc}' THEN {capture_delay_minutes[t]}\n"

        delay_query += f"ELSE {capture_delay_minutes['0']}\nEND"

    delay = (
        max(*capture_delay_minutes.values())
        if len(capture_delay_timestamps) > 0
        else capture_delay_minutes["0"]
    )

    base_query_jae = get_jae_timestamp_captura_count_query(
        engine=engine,
        delay_query=delay_query,
        capture_interval_minutes=get_capture_interval_minutes(source=source),
        capture_query=table_capture_params["query"],
        timestamp_column=timestamp_column,
        final_timestamp_exclusive=final_timestamp_exclusive,
    )

    jae_start_ts = timestamp_captura_start
    jae_result = []

    while jae_start_ts < timestamp_captura_end:
        jae_end_ts = min(jae_start_ts + timedelta(days=1), timestamp_captura_end)

        jae_start_ts_utc = jae_start_ts.astimezone(tz=timezone("UTC"))
        jae_end_ts_utc = jae_end_ts.astimezone(tz=timezone("UTC"))

        jae_end_ts_utc_format = (
            jae_end_ts_utc - timedelta(minutes=1)
            if jae_end_ts_utc < timestamp_captura_end
            else jae_end_ts_utc
        )

        query = base_query_jae.format(
            timestamp_captura_start=jae_start_ts_utc.strftime("%Y-%m-%d %H:%M:%S"),
            timestamp_captura_end=jae_end_ts_utc_format.strftime("%Y-%m-%d %H:%M:%S"),
            start=(
                jae_start_ts_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                - timedelta(minutes=delay + 1)
            ).strftime("%Y-%m-%d %H:%M:%S"),
            end=(jae_end_ts_utc + timedelta(minutes=delay))
            .replace(hour=23, minute=59, second=59, microsecond=59)
            .strftime("%Y-%m-%d %H:%M:%S"),
            delay=delay,
        )

        log(f"Executando query\n{query}")
        df_count_jae = pd.read_sql(
            sql=query,
            con=connection,
        )

        df_count_jae["timestamp_captura"] = (
            pd.to_datetime(df_count_jae["timestamp_captura"])
            .dt.tz_localize("UTC")
            .dt.tz_convert(smtr_constants.TIMEZONE.value)
        )

        jae_result.append(df_count_jae)

        jae_start_ts = jae_end_ts

    return pd.concat(jae_result)


def save_capture_check_results(env: str, results: pd.DataFrame):
    """
    Salva os resultados da verificação de captura no BigQuery.

    Args:
        env (str): dev ou prod
        results (pd.DataFrame): DataFrame contendo os resultados da verificação,
            com as seguintes colunas obrigatórias:
              - table_id (str): table_id no BigQuery
              - timestamp_captura (datetime): Data e hora da captura
              - total_datalake (int): Quantidade total de registros no datalake
              - total_jae (int): Quantidade total de registros na Jaé
              - indicador_captura_correta (bool): Se a quantidade de registros é a mesma
    """
    project_id = smtr_constants.PROJECT_NAME.value[env]
    dataset_id = f"source_{JAE_SOURCE_NAME}"
    table_id = constants.RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID.value
    results = results[
        [
            "table_id",
            "timestamp_captura",
            "total_datalake",
            "total_jae",
            "indicador_captura_correta",
        ]
    ]

    tmp_table = f"{dataset_id}.tmp_{table_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    pandas_gbq.to_gbq(
        results,
        tmp_table,
        project_id=project_id,
        if_exists="replace",
    )

    start_partition = results["timestamp_captura"].min().date().isoformat()
    end_partition = results["timestamp_captura"].max().date().isoformat()

    try:
        pandas_gbq.read_gbq(
            f"""
                MERGE {project_id}.{dataset_id}.{table_id} t
                USING {tmp_table} s
                ON
                    t.data BETWEEN '{start_partition}' AND '{end_partition}'
                    AND t.table_id = s.table_id
                    AND t.timestamp_captura = DATETIME(s.timestamp_captura, 'America/Sao_Paulo')
                WHEN MATCHED THEN
                UPDATE SET
                    total_datalake = s.total_datalake,
                    total_jae = s.total_jae,
                    indicador_captura_correta = s.indicador_captura_correta,
                    datetime_ultima_atualizacao = CURRENT_DATETIME('America/Sao_Paulo')
                WHEN
                    NOT MATCHED
                    AND DATETIME_DIFF(
                        CURRENT_DATETIME('America/Sao_Paulo'),
                        DATETIME(s.timestamp_captura, 'America/Sao_Paulo'),
                        MINUTE
                    ) > 300
                THEN
                INSERT (
                    data,
                    table_id,
                    timestamp_captura,
                    total_datalake,
                    total_jae,
                    indicador_captura_correta,
                    datetime_ultima_atualizacao
                )
                VALUES (
                    DATE(DATETIME(timestamp_captura, 'America/Sao_Paulo')),
                    table_id,
                    DATETIME(timestamp_captura, 'America/Sao_Paulo'),
                    total_datalake,
                    total_jae,
                    indicador_captura_correta,
                    CURRENT_DATETIME('America/Sao_Paulo')
                )
            """,
            project_id=project_id,
        )

    finally:

        bigquery.Client(project=project_id).delete_table(
            tmp_table,
            not_found_ok=True,
        )
