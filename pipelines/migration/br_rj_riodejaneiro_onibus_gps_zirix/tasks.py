# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_onibus_gps_zirix
"""

import traceback
from datetime import datetime, timedelta
from typing import Dict, Union

import basedosdados as bd
import pandas as pd
import pendulum
from prefect import task

from pipelines.constants import constants as smtr_constants
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import log

# Tasks #


@task
def create_api_url_onibus_realocacao(
    interval_minutes: int = 10,
    timestamp: datetime = None,
    secret_path: str = smtr_constants.ZIRIX_API_SECRET_PATH.value,
) -> str:
    """
    start_date: datahora mínima do sinal de GPS avaliado
    end_date: datahora máxima do sinal de GPS avaliado
    """

    # Configura parametros da URL
    date_range = {
        "date_range_start": (timestamp - timedelta(minutes=interval_minutes)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),
        "date_range_end": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    url = f"{smtr_constants.ZIRIX_BASE_URL.value}/EnvioViagensRetroativas?"

    headers = get_secret(secret_path)
    key = list(headers)[0]
    url = f"{url}{key}={{secret}}"

    url += f"&dataInicial={date_range['date_range_start']}&dataFinal={date_range['date_range_end']}"

    log(f"Request data from URL:\n{url}")
    return url.format(secret=headers[key])


@task
def pre_treatment_br_rj_riodejaneiro_onibus_realocacao(status: dict, timestamp: datetime) -> Dict:
    """Basic data treatment for bus gps relocation data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status (dict): dict containing the status of the request made to the
        API. Must contain keys: data and error

    Returns:
        df_realocacao: pandas.core.DataFrame containing the treated data.
    """

    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    if status["data"] == []:
        log("Data is empty, skipping treatment...")
        return {"data": pd.DataFrame(), "error": status["error"]}

    error = None

    log(f"Data received to treat: \n{status['data'][:5]}")
    df_realocacao = pd.DataFrame(status["data"])  # pylint: disable=c0103
    # df_realocacao["timestamp_captura"] = timestamp
    df_realocacao["timestamp_captura"] = timestamp.isoformat()
    # log(f"Before converting, datahora is: \n{df_realocacao['datahora']}")

    # Ajusta tipos de data
    dt_cols = [
        "dataEntrada",
        "dataOperacao",
        "dataSaida",
        "dataProcessado",
    ]

    for col in dt_cols:
        log(f"Converting column {col}")
        log(f"Data received to treat: \n{df_realocacao[col]}")
        temp_time_col_sec = pd.to_datetime(
            df_realocacao[col], format="%Y-%m-%dT%H:%M:%S", errors="coerce"
        ).dt.tz_localize(tz=smtr_constants.TIMEZONE.value)
        temp_time_col_msec = pd.to_datetime(
            df_realocacao[col], format="%Y-%m-%dT%H:%M:%S.%f", errors="coerce"
        ).dt.tz_localize(tz=smtr_constants.TIMEZONE.value)

        df_realocacao[col] = temp_time_col_sec.fillna(temp_time_col_msec).dt.strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )

        if df_realocacao[col].isna().sum() > 0:
            error = ValueError("After treating, there is null values!")
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")

        log(f"Treated data: \n{df_realocacao[col]}")

    # Ajusta tempo máximo da realocação
    df_realocacao.loc[df_realocacao.dataSaida == "1971-01-01 00:00:00-0300", "dataSaida"] = ""

    # Renomeia colunas
    cols = {
        "veiculo": "id_veiculo",
        "dataOperacao": "datetime_operacao",
        "linha": "servico",
        "dataEntrada": "datetime_entrada",
        "dataSaida": "datetime_saida",
        "dataProcessado": "timestamp_processamento",
    }

    df_realocacao = df_realocacao.rename(columns=cols)

    return {"data": df_realocacao.drop_duplicates(), "error": error}


@task
def create_api_url_onibus_gps(timestamp: datetime = None) -> str:
    """
    Generates the complete URL to get data from API.
    """

    if not timestamp:
        timestamp = pendulum.now(smtr_constants.TIMEZONE.value).replace(second=0, microsecond=0)

    headers = get_secret(smtr_constants.ZIRIX_API_SECRET_PATH.value)
    key = list(headers)[0]
    url = f"{smtr_constants.ZIRIX_BASE_URL.value}/EnvioIplan?{key}={{secret}}"

    date_range = {
        "start": (timestamp - timedelta(minutes=6)).strftime("%Y-%m-%d+%H:%M:%S"),
        "end": (timestamp - timedelta(minutes=5)).strftime("%Y-%m-%d+%H:%M:%S"),
    }
    url += f"&dataInicial={date_range['start']}&dataFinal={date_range['end']}"

    log(f"Request data from URL: {url}")
    return url.format(secret=headers[key])


@task
# pylint: disable=too-many-locals
def pre_treatment_br_rj_riodejaneiro_onibus_gps(
    status: dict, timestamp: datetime, version: int = 1, recapture: bool = False
) -> Dict:
    """Basic data treatment for bus gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data and error
        version (int, optional): Source API version. Temporary argument
        for testing
        timestamp (str): Capture data timestamp.

    Returns:
        df_gps: pandas.core.DataFrame containing the treated data.
    """
    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    if status["data"] == []:
        log("Data is empty, skipping treatment...")
        return {"data": pd.DataFrame(), "error": status["error"]}

    error = None
    timezone = smtr_constants.TIMEZONE.value

    log(f"Data received to treat: \n{status['data'][:5]}")
    df_gps = pd.DataFrame(status["data"])  # pylint: disable=c0103
    df_gps["timestamp_captura"] = timestamp
    log(f"Before converting, datahora is: \n{df_gps['datahora']}")

    # Remove timezone and force it to be config timezone
    if version == 1:
        timestamp_cols = ["datahora"]
    elif version == 2:
        timestamp_cols = ["datahora", "datahoraenvio"]
    if recapture:
        timestamp_cols.append("datahoraservidor")
    for col in timestamp_cols:
        print(f"Before converting, {col} is: \n{df_gps[col].head()}")  # log
        df_gps[col] = (
            pd.to_datetime(df_gps[col].astype(float), unit="ms")
            .dt.tz_localize(tz="UTC")
            .dt.tz_convert(timezone)
        )
        log(f"After converting the timezone, {col} is: \n{df_gps[col].head()}")

    # Filter data
    try:
        log(f"Shape before filtering: {df_gps.shape}")
        if version == 1:
            filter_col = "timestamp_captura"
            time_delay = smtr_constants.GPS_SPPO_CAPTURE_DELAY_V1.value
        elif version == 2:
            filter_col = "datahoraenvio"
            time_delay = smtr_constants.GPS_SPPO_CAPTURE_DELAY_V2.value
        if recapture:
            server_mask = (df_gps["datahoraenvio"] - df_gps["datahoraservidor"]) <= timedelta(
                minutes=smtr_constants.GPS_SPPO_RECAPTURE_DELAY_V2.value
            )
            df_gps = df_gps[server_mask]  # pylint: disable=c0103

        mask = (df_gps[filter_col] - df_gps["datahora"]).apply(
            lambda x: timedelta(seconds=-20) <= x <= timedelta(minutes=time_delay)
        )

        # Select and drop duplicated data
        cols = [
            "ordem",
            "latitude",
            "longitude",
            "datahora",
            "velocidade",
            "linha",
            "timestamp_captura",
        ]
        df_gps = df_gps[mask][cols]  # pylint: disable=c0103
        df_gps = df_gps.drop_duplicates(  # pylint: disable=c0103
            ["ordem", "latitude", "longitude", "datahora", "timestamp_captura"]
        )

        log(f"Shape after filtering: {df_gps.shape}")
        if df_gps.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")
    except Exception:  # pylint: disable = W0703
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": df_gps, "error": error}


@task
def clean_br_rj_riodejaneiro_onibus_gps_zirix(date_range: dict) -> Union[str, None]:
    """
    Clean GPS data for a given date range.

    This function deletes records from three different tables in the database:
    - `rj-smtr.br_rj_riodejaneiro_onibus_gps_zirix.sppo_aux_registro_filtrada`
    - `rj-smtr.br_rj_riodejaneiro_onibus_gps_zirix.sppo_aux_registros_realocacao`
    - `rj-smtr.br_rj_riodejaneiro_onibus_gps_zirix.gps_sppo`

    The records to be deleted are determined by the provided
    date range and the timestamp_gps column.

    Parameters:
        - date_range (dict): A dictionary containing the start
        and end dates for the data to be cleaned.

    Returns:
        - str or None: If an error occurs during the cleaning process,
            the error message is returned. Otherwise, None is returned.

    """
    error = None

    try:
        q = f"""
            DELETE
            FROM
                `rj-smtr.br_rj_riodejaneiro_onibus_gps_zirix.sppo_aux_registro_filtrada`
            WHERE
                (data BETWEEN DATE("{date_range['date_range_start']}")
                    AND DATE("{date_range['date_range_end']}"))
                AND (timestamp_gps > "{date_range['date_range_start']}"
                    AND timestamp_gps <= "{date_range['date_range_end']}");
            DELETE
            FROM
                `rj-smtr.br_rj_riodejaneiro_onibus_gps_zirix.sppo_aux_registros_realocacao`
            WHERE
                (data BETWEEN DATE("{date_range['date_range_start']}")
                    AND DATE("{date_range['date_range_end']}"))
                AND (timestamp_gps > "{date_range['date_range_start']}"
                    AND timestamp_gps <= "{date_range['date_range_end']}");
            DELETE
            FROM
                `rj-smtr.br_rj_riodejaneiro_onibus_gps_zirix.gps_sppo`
            WHERE
                (data BETWEEN DATE("{date_range['date_range_start']}")
                    AND DATE("{date_range['date_range_end']}"))
                AND (timestamp_gps > "{date_range['date_range_start']}"
                    AND timestamp_gps <= "{date_range['date_range_end']}");
            """
        log(q)

        results = bd.read_sql(q)

        log(
            f"""Cleaned GPS data for
                {date_range['date_range_start']} to {date_range['date_range_end']}\n
                Resulting:\n
                {results}"""
        )
    except Exception:  # pylint: disable = W0703
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error
