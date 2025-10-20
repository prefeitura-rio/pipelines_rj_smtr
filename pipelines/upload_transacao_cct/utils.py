# -*- coding: utf-8 -*-
from datetime import datetime

import pandas_gbq
from google.cloud.storage import Blob
from prefeitura_rio.pipelines_utils.logging import log
from psycopg2._psycopg import cursor

from pipelines.upload_transacao_cct.constants import constants


def get_modified_partitions(
    project_id: str,
    start_ts: str,
    end_ts: str,
) -> list[str]:
    """
    Retorna as partições da tabela `rj-smtr.bilhetagem.transacao`
    que foram modificadas entre dois timestamps.

    Args:
        project_id (str): ID do projeto no BigQuery.
        start_ts (str): Timestamp inicial no formato compatível com BigQuery.
        end_ts (str): Timestamp final no formato compatível com BigQuery.

    Returns:
        list[str]: Lista de partições modificadas no intervalo informado.
    """

    sql = f"""
    SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS particao
    FROM
        `rj-smtr.bilhetagem.INFORMATION_SCHEMA.PARTITIONS`
    WHERE
        table_name = "transacao"
        AND partition_id != "__NULL__"
        AND DATETIME(last_modified_time, "America/Sao_Paulo")
            BETWEEN DATETIME("{start_ts}")
            AND DATETIME("{end_ts}")
    """

    log(f"Executando query:\n{sql}")

    modified_partitions = pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()

    sql = f"""
        SELECT
            DISTINCT CONCAT("'", data, "'") AS particao
        FROM
            {project_id}.{constants.TRANSACAO_CCT_VIEW_NAME.value}
        WHERE
        data IN ({', '.join(modified_partitions)})
        AND datetime_ultima_atualizacao
            BETWEEN DATETIME("{start_ts}")
            AND DATETIME("{end_ts}")
    """

    log(f"Executando query:\n{sql}")
    return pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()


def get_partition_using_data_ordem(
    project_id: str,
    data_ordem_start: str,
    data_ordem_end: str,
) -> list[str]:
    """
    Retorna as partições da tabela `rj-smtr.bilhetagem.transacao`
    associadas a um intervalo de datas de ordem.

    Args:
        project_id (str): ID do projeto no BigQuery.
        data_ordem_start (str): Data da ordem de pagamento inicial para filtrar as partições.
        data_ordem_end (str): Data da ordem de pagamento final para filtrar as partições.

    Returns:
        list[str]: Lista de partições correspondentes ao intervalo de datas de ordem.
    """

    try:
        datetime.strptime(data_ordem_start, "%Y-%m-%d")
        datetime.strptime(data_ordem_end, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Formato de data_ordem inválido: {e}")

    sql = f"""
    SELECT
        DISTINCT CONCAT("'", data_transacao, "'") AS particao
    FROM
        `rj-smtr.bilhetagem_interno.data_ordem_transacao`
    WHERE
        data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
    """

    log(f"Executando query:\n{sql}")

    partitions = pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()

    sql = f"""
        SELECT
            DISTINCT CONCAT("'", data, "'") AS particao
        FROM
            {project_id}.{constants.TRANSACAO_CCT_VIEW_NAME.value}

        WHERE
        data IN ({', '.join(partitions)})
        AND data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
    """

    log(f"Executando query:\n{sql}")
    return pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()


def create_temp_table(cur: cursor, blob: Blob, full_refresh: bool):
    """
    Cria e popula a tabela temporária no PostgreSQL com os dados de um arquivo CSV no GCS.

    Args:
        cur (cursor): Cursor ativo da conexão PostgreSQL.
        blob (Blob): Objeto Blob que representa o arquivo CSV no GCS.
        full_refresh (bool): Indica se o carregamento é completo (True) ou incremental (False).
    """

    if not full_refresh:
        tmp_table_name = constants.TRANSACAO_POSTGRES_TMP_TABLE_NAME.value
        sql = f"""
            CREATE TABLE IF NOT EXISTS public.{tmp_table_name}
            (
                id_transacao character varying(60),
                data date,
                datetime_transacao timestamp,
                consorcio character varying(20),
                tipo_transacao character varying(50),
                valor_pagamento numeric(13,5),
                data_ordem date,
                id_ordem_pagamento integer,
                id_ordem_pagamento_consorcio_operador_dia integer,
                datetime_ultima_atualizacao timestamp,
                datetime_export timestamp
            )
        """
        log("Criando tabela temporária")
        cur.execute(sql)
        log("Tabela temporária criada")

        sql = f"DROP INDEX IF EXISTS public.{constants.TMP_TABLE_INDEX_NAME.value}"
        log("Deletando índice da tabela temporária")
        cur.execute(sql)

        log(f"Copiando arquivo {blob.name} para a tabela temporária")
        sql = f"""
            COPY public.{tmp_table_name}
            FROM STDIN WITH CSV HEADER
        """

        with blob.open("r") as f:
            cur.copy_expert(sql, f)
        log("Cópia completa")

        sql = f"""
            CREATE INDEX {constants.TMP_TABLE_INDEX_NAME.value}
            ON public.{tmp_table_name} (id_transacao)
        """
        log("Criando índice tabela temporária")
        cur.execute(sql)


def merge_final_data(cur: cursor, blob: Blob, full_refresh: bool):
    """
    Atualiza a tabela final no PostgreSQL com base em um arquivo CSV no GCS,
    substituindo registros existentes.

    Args:
        cur (cursor): Cursor ativo da conexão PostgreSQL.
        blob (Blob): Objeto Blob que representa o arquivo CSV no GCS.
        full_refresh (bool): Indica se o carregamento é completo (True) ou incremental (False).
    """

    table_name = constants.TRANSACAO_POSTGRES_TABLE_NAME.value
    tmp_table_name = constants.TRANSACAO_POSTGRES_TMP_TABLE_NAME.value

    if not full_refresh:
        sql = f"""
            DELETE FROM public.{table_name} t
            USING public.{tmp_table_name} s
            WHERE t.id_transacao = s.id_transacao
        """
        log("Deletando registros da tabela final")
        cur.execute(sql)
        log(f"{cur.rowcount} linhas deletadas")

    log("Deletando tabela temporária")
    cur.execute(f"DROP TABLE IF EXISTS public.{tmp_table_name}")
    log("Tabela temporária deletada")

    sql = f"DROP INDEX IF EXISTS public.{constants.FINAL_TABLE_ID_TRANSACAO_INDEX_NAME.value}"
    log("Deletando índice id_transacao da tabela final")
    cur.execute(sql)

    log(f"Copiando arquivo {blob.name} para a tabela final")
    sql = f"""
        COPY public.{table_name}
        FROM STDIN WITH CSV HEADER
    """
    with blob.open("r") as f:
        cur.copy_expert(sql, f)
    log("Cópia completa")

    sql = f"""
        CREATE INDEX {constants.FINAL_TABLE_ID_TRANSACAO_INDEX_NAME.value}
        ON public.{table_name} (id_transacao)
    """
    log("Recriando índice id_transacao da tabela final")
    cur.execute(sql)
