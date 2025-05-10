# -*- coding: utf-8 -*-
import traceback
from datetime import datetime, timedelta
from typing import List

import pandas as pd

from pipelines.constants import constants as smtr_constants
from pipelines.utils.utils import log


def pretreat_cittati_registros(
    data: pd.DataFrame,
    timestamp: datetime,  # pylint: disable=W0613
    primary_keys: List[str],  # pylint: disable=W0613
) -> pd.DataFrame:
    """
    Basic data treatment for bus gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        data (pd.DataFrame): DataFrame with the data from CITTATI API

    Returns:
        pd.DataFrame: Treated dataframe
    """

    timezone = smtr_constants.TIMEZONE.value

    try:
        dt_cols = ["datetime", "datetime_envio", "datetime_servidor"]

        for col in dt_cols:
            if col in data.columns:
                log(f"Before converting, {col} is: \n{data[col].head()}")

                if pd.api.types.is_datetime64_dtype(data[col]):
                    data[col] = (
                        data[col]
                        .dt.tz_localize(None)
                        .dt.tz_localize(timezone)
                        .dt.strftime("%Y-%m-%d %H:%M:%S%z")
                    )
                else:
                    data[col] = (
                        pd.to_datetime(data[col])
                        .dt.tz_localize(None)
                        .dt.tz_localize(timezone)
                        .dt.strftime("%Y-%m-%d %H:%M:%S%z")
                    )

                log(f"After converting the timezone, {col} is: \n{data[col].head()}")

        log(f"Shape before filtering: {data.shape}")

        filter_col = "datetime_envio"
        time_delay = 60

        temp_dt_envio = pd.to_datetime(data[filter_col])
        temp_dt = pd.to_datetime(data["datetime"])
        mask = (temp_dt_envio - temp_dt).apply(
            lambda x: timedelta(seconds=-20) <= x <= timedelta(minutes=time_delay)
        )

        cols = [
            "id_veiculo",
            "servico",
            "sentido",
            "latitude",
            "longitude",
            "datetime",
            "velocidade",
            "direcao",
            "route_id",
            "trip_id",
            "shape_id",
            "datetime_envio",
            "datetime_servidor",
        ]

        filtered_data = data[mask][cols]
        filtered_data = filtered_data.drop_duplicates(
            ["id_veiculo", "latitude", "longitude", "datetime", "datetime_servidor"]
        )

        log(f"Shape after filtering: {filtered_data.shape}")

        if filtered_data.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            log(f"[CATCHED] Task failed with error: \n{error}", level="error")
            return pd.DataFrame()

        return filtered_data

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")
        return pd.DataFrame()


def pretreat_cittati_realocacao(
    data: pd.DataFrame,
    timestamp: datetime,  # pylint: disable=W0613
    primary_keys: List[str],  # pylint: disable=W0613
) -> pd.DataFrame:
    """
    Basic data treatment for bus gps relocation data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        data (pd.DataFrame): DataFrame with the data from CITTATI API

    Returns:
        pd.DataFrame: Treated dataframe
    """

    timezone = smtr_constants.TIMEZONE.value

    try:
        dt_cols = [
            "datetime_entrada",
            "datetime_operacao",
            "datetime_saida",
            "datetime_processamento",
        ]

        for col in dt_cols:
            if col in data.columns:
                log(f"Before converting, {col} is: \n{data[col].head()}")

                if pd.api.types.is_datetime64_dtype(data[col]):
                    data[col] = (
                        data[col]
                        .dt.tz_localize(None)
                        .dt.tz_localize(timezone)
                        .dt.strftime("%Y-%m-%d %H:%M:%S%z")
                    )
                else:
                    data[col] = (
                        pd.to_datetime(data[col])
                        .dt.tz_localize(None)
                        .dt.tz_localize(timezone)
                        .dt.strftime("%Y-%m-%d %H:%M:%S%z")
                    )

                if data[col].isna().sum() > 0:
                    error = ValueError("After treating, there is null values!")
                    log(f"[CATCHED] Task failed with error: \n{error}", level="error")

                log(f"After converting the timezone, {col} is: \n{data[col].head()}")

        if "datetime_saida" in data.columns:
            data.loc[data.datetime_saida == "1971-01-01 00:00:00-0300", "datetime_saida"] = ""

        return data.drop_duplicates()

    except Exception:
        error = traceback.format_exc()
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")
        return pd.DataFrame()
