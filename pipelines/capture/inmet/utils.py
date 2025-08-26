# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from pipelines.utils.utils import convert_timezone


def pretreatment_inmet(
    data: pd.DataFrame,
    timestamp: datetime,
    primary_keys: list[str],
) -> pd.DataFrame:
    """
    Trata dados meteorológicos do INMET:
    - Renomeia colunas para nomes mais consistentes
    - Converte timezone UTC -> America/Sao_Paulo
    - Ajusta formato de data e hora
    - Ordena colunas com base nas primary_keys
    - Filtra dados que tenham a mesma hora que o timestamp fornecido

    Args:
        data (pd.DataFrame): DataFrame de entrada com dados do INMET.
        timestamp (datetime): Timestamp de referência para filtrar os registros
                              com a mesma hora (considerando timezone UTC).
        primary_keys (list[str]): Colunas-chave para ordenação no DataFrame final.

    Returns:
        pd.DataFrame: Dados tratados e filtrados pelo timestamp.
    """

    # Remove colunas
    drop_cols = [
        "TEM_SEN",
        "TEN_BAT",
        "TEM_CPU",
    ]
    data = data.drop([c for c in drop_cols if c in data.columns], axis=1)

    # Renomeia colunas
    rename_cols = {
        "DC_NOME": "estacao",
        "UF": "sigla_uf",
        "VL_LATITUDE": "latitude",
        "VL_LONGITUDE": "longitude",
        "CD_ESTACAO": "id_estacao",
        "VEN_DIR": "direcao_vento",
        "DT_MEDICAO": "data",
        "HR_MEDICAO": "horario",
        "VEN_RAJ": "rajada_vento_max",
        "CHUVA": "acumulado_chuva_1_h",
        "PRE_INS": "pressao",
        "PRE_MIN": "pressao_minima",
        "PRE_MAX": "pressao_maxima",
        "UMD_INS": "umidade",
        "UMD_MIN": "umidade_minima",
        "UMD_MAX": "umidade_maxima",
        "VEN_VEL": "velocidade_vento",
        "TEM_INS": "temperatura",
        "TEM_MIN": "temperatura_minima",
        "TEM_MAX": "temperatura_maxima",
        "RAD_GLO": "radiacao_global",
        "PTO_INS": "temperatura_orvalho",
        "PTO_MIN": "temperatura_orvalho_minimo",
        "PTO_MAX": "temperatura_orvalho_maximo",
    }
    data = data.rename(columns=rename_cols)

    # Converte coluna de horas (ex.: 2300 -> 23:00:00)
    data["horario"] = data["horario"].astype(str).str.zfill(4)
    data["horario"] = pd.to_datetime(data["horario"], format="%H%M")
    data["horario"] = data["horario"].dt.strftime("%H:%M:%S")

    # Converte timezone
    data["datetime"] = pd.to_datetime(data["data"] + " " + data["horario"], utc=True)
    data["datetime"] = data["datetime"].apply(convert_timezone)
    data["data"] = data["datetime"].dt.strftime("%Y-%m-%d")
    data["horario"] = data["datetime"].dt.strftime("%H:%M:%S")
    data = data.drop(columns=["datetime"])

    # Ordena colunas
    cols = [c for c in data.columns if c not in primary_keys]
    data = data[primary_keys + cols]

    # Converte colunas numéricas
    float_cols = [
        "pressao",
        "pressao_maxima",
        "radiacao_global",
        "temperatura_orvalho",
        "temperatura_minima",
        "umidade_minima",
        "temperatura_orvalho_maximo",
        "direcao_vento",
        "acumulado_chuva_1_h",
        "pressao_minima",
        "umidade_maxima",
        "velocidade_vento",
        "temperatura_orvalho_minimo",
        "temperatura_maxima",
        "rajada_vento_max",
        "temperatura",
        "umidade",
    ]

    for col in float_cols:
        if col in data.columns:
            data[col] = data[col].replace(["", "null"], np.nan)
            data[col] = data[col].astype(float)

    timestamp_filtro = timestamp - timedelta(days=1)
    data_filtro = timestamp_filtro.strftime("%Y-%m-%d")
    data = data[data["data"] == data_filtro]

    # Remove linhas totalmente vazias
    data = data.dropna(subset=[c for c in float_cols if c in data.columns], how="all")

    return data
