# -*- coding: utf-8 -*-
"""Tasks para exportação das transações do BQ para o Postgres"""
from datetime import datetime
from typing import Optional

import pandas_gbq
import psycopg2
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.capture.cct.constants import CCT_PRIVATE_BUCKET_NAMES
from pipelines.capture.cct.constants import constants as cct_constants
from pipelines.constants import constants as smtr_constants
from pipelines.upload_transacao_cct.constants import constants
from pipelines.upload_transacao_cct.utils import (
    create_temp_table,
    get_modified_partitions,
    get_partition_using_data_ordem,
    merge_final_data,
)
from pipelines.utils.gcp.storage import Storage
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import convert_timezone


@task(nout=2)
def get_start_datetime(
    env: str,
    full_refresh: bool,
    data_ordem_start: Optional[str],
    data_ordem_end: Optional[str],
) -> tuple[Optional[datetime], bool]:
    """
    Obtém o timestamp da última execução a partir do Redis, ajustando o modo de execução.

    Args:
        env (str): dev ou prod.
        full_refresh (bool): Indica se a execução é um carregamento completo.
        data_ordem_start (Optional[str]): Data inicial do filtro de ordem.
        data_ordem_end (Optional[str]): Data final do filtro de ordem.

    Returns:
        tuple[Optional[datetime], bool]: Timestamp inicial e flag indicando se é full refresh.
    """

    if (data_ordem_start is None) != (data_ordem_end is None):
        raise ValueError("Filtro de data ordem com inicio ou fim nulo")

    start_datetime = None
    if not full_refresh and data_ordem_start is None:
        redis_client = get_redis_client()
        content = redis_client.get(f"{env}.{constants.REDIS_KEY.value}")
        if content is None:
            full_refresh = True
        else:
            start_datetime = convert_timezone(
                timestamp=datetime.fromisoformat(
                    content[constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME.value]
                )
            )

    return start_datetime, full_refresh


@task
def full_refresh_delete_all_files(env: str):
    """
    Remove todos os arquivos exportados do GCS.

    Args:
        env (str): dev ou prod.
    """

    log("FULL REFRESH: Deletando todos os arquivos do GCS")
    storage = Storage(
        env=env,
        dataset_id=constants.TRANSACAO_CCT_FOLDER.value,
        bucket_names=CCT_PRIVATE_BUCKET_NAMES,
    )

    for b in storage.bucket.list_blobs(prefix=f"{constants.EXPORT_GCS_PREFIX.value}/"):
        b.delete()


@task
def export_data_from_bq_to_gcs(
    env: str,
    timestamp: datetime,
    start_datetime: datetime,
    full_refresh: bool,
    data_ordem_start: Optional[str],
    data_ordem_end: Optional[str],
):
    """
    Exporta dados do BigQuery para o GCS aplicando filtros conforme o tipo de execução.

    Args:
        env (str): dev ou prod.
        timestamp (datetime): Timestamp de referência da execução.
        start_datetime (datetime): Timestamp inicial para filtragem incremental.
        full_refresh (bool): Indica se o carregamento é completo.
        data_ordem_start (Optional[str]): Data inicial do filtro de ordem.
        data_ordem_end (Optional[str]): Data final do filtro de ordem.
    """
    project_id = smtr_constants.PROJECT_NAME.value[env]

    file_name = f"{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}-*.csv"

    export_filter = "true"

    end_ts = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    if full_refresh:
        export_filter = f"datetime_ultima_atualizacao <= DATETIME('{end_ts}')"
    else:
        if data_ordem_start is None:
            start_ts = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
            modified_partitions = get_modified_partitions(
                project_id=project_id,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            export_filter = f"""
            data IN ({', '.join(modified_partitions)})
            AND datetime_ultima_atualizacao
                BETWEEN DATETIME("{start_ts}")
                AND DATETIME("{end_ts}")
            """
        else:
            partitions = get_partition_using_data_ordem(
                project_id=project_id,
                data_ordem_start=data_ordem_start,
                data_ordem_end=data_ordem_end,
            )
            export_filter = f"""
            data IN ({', '.join(partitions)})
            AND data_ordem
                BETWEEN "{data_ordem_start}"
                AND "{data_ordem_end}"
            """

    transacao_select = f"""
        SELECT
            {{cols}}
        FROM
            {project_id}.{constants.TRANSACAO_CCT_VIEW_NAME.value}
        WHERE
            {export_filter}
        LIMIT {{count}}
    """

    sql = transacao_select.format(cols="count(1) as ct", count=1)
    log(f"Executando query:\n{sql}")
    limit = pandas_gbq.read_gbq(sql, project_id=project_id)["ct"].max()
    export_uri = (
        f"gs://{CCT_PRIVATE_BUCKET_NAMES[env]}/{constants.EXPORT_GCS_PREFIX.value}/{file_name}"
    )
    cols = "*, current_datetime('America/Sao_Paulo') as datetime_export"
    sql = f"""
        EXPORT DATA
        OPTIONS(uri='{export_uri}',
        format='CSV',
        OVERWRITE=TRUE,
        header=TRUE) AS

        {transacao_select.format(cols=cols, count=limit)}
    """
    log(f"Executando query:\n{sql}")
    pandas_gbq.read_gbq(sql, project_id=project_id)


@task
def upload_files_postgres(
    env: str,
    full_refresh: bool,
):
    """
    Carrega os arquivos CSV das transações no GCS para o PostgreSQL

    Args:
        env (str): dev ou prod.
        full_refresh (bool): Indica se o carregamento é completo (truncate + reload) ou incremental.
    """
    table_name = constants.TRANSACAO_POSTGRES_TABLE_NAME.value

    credentials = (
        get_secret(cct_constants.CCT_SECRET_PATH.value)
        if env == "prod"
        else get_secret(cct_constants.CCT_HMG_SECRET_PATH.value)
    )

    blobs = Storage(
        env=env,
        dataset_id="transacao_cct",
        bucket_names=CCT_PRIVATE_BUCKET_NAMES,
    ).bucket.list_blobs(prefix=f"{constants.EXPORT_GCS_PREFIX.value}/")

    with psycopg2.connect(
        host=credentials["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["dbname"],
    ) as conn:
        conn.autocommit = False

        with conn.cursor() as cur:

            sql = f"DROP INDEX IF EXISTS public.{constants.FINAL_TABLE_ID_ORDEM_INDEX_NAME.value}"
            log("Deletando índice ordem pagamento da tabela final")
            cur.execute(sql)
            log("Índice deletado")

            sql = f"DROP INDEX IF EXISTS public.{constants.FINAL_TABLE_EXPORT_INDEX_NAME.value}"
            log("Deletando índice datetime export da tabela final")
            cur.execute(sql)
            log("Índice deletado")

            if full_refresh:
                log("Truncando tabela final")
                cur.execute(f"TRUNCATE TABLE public.{table_name}")

            for blob in blobs:

                create_temp_table(cur=cur, blob=blob, full_refresh=full_refresh)

                merge_final_data(cur=cur, blob=blob, full_refresh=full_refresh)

            sql = f"""
                CREATE INDEX
                {constants.FINAL_TABLE_ID_ORDEM_INDEX_NAME.value}
                ON public.{table_name} (id_ordem_pagamento_consorcio_operador_dia)
            """
            log("Recriando índice ordem pagamento da tabela final")
            cur.execute(sql)
            log("Índice recriado")

            sql = f"""
                CREATE INDEX
                {constants.FINAL_TABLE_EXPORT_INDEX_NAME.value}
                ON public.{table_name} (datetime_export)
            """
            log("Recriando índice datetime export da tabela final")
            cur.execute(sql)
            log("Índice recriado")

        conn.commit()

        for blob in blobs:
            log("Deletando arquivo do GCS")
            blob.delete()
            log("Arquivo do GCS deletado")


@task
def save_upload_timestamp_redis(
    env: str,
    timestamp: datetime,
    data_ordem_start: Optional[str],
):
    """
    Atualiza o timestamp da execução no Redis.

    Args:
        env (str): dev ou prod.
        timestamp (datetime): Timestamp de referência da execução.
        data_ordem_start (Optional[str]): Data inicial do parâmetro de filtro de ordem.
    """

    if data_ordem_start is not None:
        return

    value = timestamp.isoformat()
    redis_key = f"{env}.{constants.REDIS_KEY.value}"
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)

    log(f"Salvando timestamp {value} na chave: {redis_key}")
    if not content:
        content = {constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME.value: value}
        log("Timestamp salva no Redis")
        redis_client.set(redis_key, content)
    else:
        if (
            convert_timezone(
                datetime.fromisoformat(content[constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME.value])
            )
            < timestamp
        ):
            content[constants.LAST_UPLOAD_TIMESTAMP_KEY_NAME.value] = value
            log("Timestamp salva no Redis")
            redis_client.set(redis_key, content)
