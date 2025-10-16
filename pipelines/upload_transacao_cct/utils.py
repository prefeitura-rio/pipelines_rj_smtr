# -*- coding: utf-8 -*-
import pandas_gbq
from prefeitura_rio.pipelines_utils.logging import log


def get_modified_partitions(
    project_id: str,
    start_ts: str,
    end_ts: str,
) -> list[str]:
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
            rj-smtr.projeto_app_cct.transacao_cct
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
            DISTINCT CONCAT("'", t.data, "'") AS particao
        FROM
            rj-smtr.projeto_app_cct.transacao_cct t
        JOIN
            rj-smtr.financeiro.bilhetagem_consorcio_operador_dia o
        USING(id_ordem_pagamento_consorcio_operador_dia)
        WHERE
        t.data IN ({', '.join(partitions)})
        AND o.data_ordem BETWEEN "{data_ordem_start}" AND "{data_ordem_end}"
    """

    log(f"Executando query:\n{sql}")
    return pandas_gbq.read_gbq(
        sql,
        project_id=project_id,
    )["particao"].to_list()
