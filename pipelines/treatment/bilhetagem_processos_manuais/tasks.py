# -*- coding: utf-8 -*-
"""Tasks para os processos manuais de bilhetagem"""
import pandas_gbq
from prefect import task

from pipelines.capture.jae.constants import JAE_SOURCE_NAME
from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.bilhetagem_processos_manuais.constants import constants


@task
def get_gaps_from_result_table(
    env: str, table_ids: list[str], timestamp_start: str, timestamp_end: str
) -> dict[list[str], bool]:
    project_id = smtr_constants.PROJECT_NAME.value[env]
    dataset_id = f"source_{JAE_SOURCE_NAME}"
    result = {}
    query = f"""
        select
            table_id,
            format_datetime(timestamp_captura, '%Y-%m-%d %H:%M:%S') as timestamp_captura
        from
            {project_id}.{dataset_id}.{jae_constants.RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID.name}
        where
            data between date('{timestamp_start}') and date('{timestamp_end}')
            and
                timestamp_captura
                between datetime('{timestamp_start}') and datetime('{timestamp_end}')
            and not indicador_captura_correta
            and table_id in ({', '.join([f"'{t}'" for t in table_ids])})
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
def create_gap_materialization_params(gaps: dict[list[str], bool]) -> dict:
    result = {}
    for k, v in constants.CAPTURE_GAP_SELECTORS.value.items():
        ts_list = []

        if any([gaps[a]["flag_has_gaps"] for a in v["capture_tables"]]):
            for t in v["capture_tables"]:
                ts_list = ts_list + gaps[t]["timestamps"]

            result[k] = {
                "initial_datetime": min(ts_list),
                "end_datetime": max(ts_list),
            }

    return result
