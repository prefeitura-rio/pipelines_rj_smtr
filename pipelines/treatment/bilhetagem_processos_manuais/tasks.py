# -*- coding: utf-8 -*-
"""Tasks para os processos manuais de bilhetagem"""
from datetime import datetime, timedelta

import pandas_gbq
from prefect import task

from pipelines.capture.jae.constants import JAE_SOURCE_NAME
from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.bilhetagem_processos_manuais.constants import constants
from pipelines.utils.utils import convert_timezone


@task
def create_transacao_ordem_capture_params(timestamp: str) -> dict:
    return {
        "timestamp": jae_constants.TRANSACAO_ORDEM_SOURCE.value.get_last_scheduled_timestamp(
            timestamp=convert_timezone(datetime.fromisoformat(timestamp))
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "recapture": False,
    }


@task
def get_gaps_from_result_table(
    env: str,
    table_ids: list[str],
    timestamp_start: str,
    timestamp_end: str,
) -> dict:
    """
    Obtém informações sobre timestamps com dados divergentes na captura da Jaé

    Args:
        env (str): dev ou prod
        table_ids (list[str]): Lista de tabelas capturadas a serem verificadas
        timestamp_start (str): Timestamp inicial no formato `YYYY-MM-DD HH:MM:SS`
        timestamp_end (str): Timestamp final no formato `YYYY-MM-DD HH:MM:SS`

    Returns:
        dict: Dicionário onde cada chave é um `table_id` e o valor é outro dicionário com:
            - `"timestamps"` (list[str]): Lista de timestamps das falhas de captura
            - `"flag_has_gaps"` (bool): Indica se houve lacunas de captura
    """
    project_id = smtr_constants.PROJECT_NAME.value[env]
    dataset_id = f"source_{JAE_SOURCE_NAME}"
    result = {}
    date_filter = (
        f"""
        and data between date('{timestamp_start}') and date('{timestamp_end}')
        and
            timestamp_captura
            between datetime('{timestamp_start}') and datetime('{timestamp_end}')
    """
        if timestamp_start is not None and timestamp_end is not None
        else ""
    )
    query = f"""
        select
            table_id,
            format_datetime('%Y-%m-%d %H:%M:%S', timestamp_captura) as timestamp_captura
        from
            {project_id}.{dataset_id}.{jae_constants.RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID.value}
        where
            not indicador_captura_correta
            and table_id in ({', '.join([f"'{t}'" for t in table_ids])})
            {date_filter}
        """
    df_bq = pandas_gbq.read_gbq(query, project_id=project_id)
    for table_id in table_ids:
        df = df_bq[df_bq["table_id"] == table_id]
        result[table_id] = {
            "timestamps": df["timestamp_captura"].to_list(),
            "flag_has_gaps": not df.empty,
        }
    return result


@task
def create_gap_materialization_params(gaps: dict) -> dict:
    """
    Cria parâmetros de materialização a partir das falhas de captura identificadas

    Args:
        gaps (dict): Dicionário com informações de gaps por tabela, no formato:
            {
                "table_id": {
                    "timestamps": list[str],
                    "flag_has_gaps": bool
                },
                ...
            }
    """
    result = {}
    for k, v in constants.CAPTURE_GAP_SELECTORS.value.items():
        ts_list = []

        if any(gaps[a]["flag_has_gaps"] for a in v["capture_tables"]):
            for t in v["capture_tables"]:
                ts_list = ts_list + gaps[t]["timestamps"]

            result[k] = {
                "initial_datetime": min(ts_list),
                "end_datetime": max(ts_list),
            }

    return result


@task
def create_verify_capture_params(gaps: dict) -> list[dict]:
    """
    Gera intervalos de parâmetros para verificação de captura com base em datas de gaps.

    Args:
        gaps (dict): Dicionário com informações de gaps por tabela, no formato:
            {
                "table_id": {
                    "timestamps": list[str],  # Timestamps ISO em string
                    "flag_has_gaps": bool
                },
                ...
            }

    Returns:
        list[dict]: Lista de parâmetros a serem executados
    """
    dates = sorted(
        list(
            set([datetime.fromisoformat(t).date() for v in gaps.values() for t in v["timestamps"]])
        )
    )

    params = []
    param = {}
    last_date = None
    for d in dates:
        if last_date is None:
            param = {"timestamp_captura_start": d.strftime("%Y-%m-%d 00:00:00")}
        elif last_date != d - timedelta(days=1):
            param["timestamp_captura_end"] = last_date.strftime("%Y-%m-%d 23:59:59")
            params.append(param)
            param = {"timestamp_captura_start": d.strftime("%Y-%m-%d 00:00:00")}

        last_date = d
    param["timestamp_captura_end"] = last_date.strftime("%Y-%m-%d 23:59:59")
    params.append(param)

    return params
