# -*- coding: utf-8 -*-
"""Funções auxiliares para captura de dados do STU"""
import json
from datetime import datetime, timedelta
from io import StringIO
from typing import List

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.capture.stu.constants import STU_PRIVATE_BUCKET_NAMES, STU_SOURCE_NAME
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.gcp.storage import Storage


def processa_dados(blobs: List, date_str: str) -> pd.DataFrame:
    """
    Carrega e processa dados de uma data específica do bucket.

    Args:
        blobs: Lista de blobs disponíveis no bucket
        date_str: Data no formato YYYY_MM_DD

    Returns:
        pd.DataFrame: Dados processados da data especificada
    """
    # Filtra arquivos da data específica
    files = [
        blob
        for blob in blobs
        if blob.name.split("/")[-1].startswith(date_str) and blob.name.endswith(".csv")
    ]

    if not files:
        log(f"Nenhum arquivo encontrado para {date_str}")
        return pd.DataFrame()

    log(f"Processando {len(files)} arquivos para {date_str}")

    aux_df = []
    for file in files:
        try:
            filename = file.name.split("/")[-1]
            log(f"Lendo arquivo: {filename}")

            # Lê direto do blob
            csv_string = file.download_as_text()

            # Converte string para DataFrame
            df = pd.read_csv(StringIO(csv_string), dtype=str)

            # Processa a coluna _airbyte_data que contém JSON
            if "_airbyte_data" in df.columns:
                data = pd.json_normalize(df["_airbyte_data"].apply(json.loads))
                aux_df.append(data)
            else:
                log(f"Arquivo {filename} não contém coluna _airbyte_data")

        except Exception as e:
            log(f"Erro ao processar arquivo {file.name}: {str(e)}", level="error")
            continue

    if not aux_df:
        return pd.DataFrame()

    result = pd.concat(aux_df, ignore_index=True)

    # Remove duplicatas baseado em todas as colunas
    result = result.drop_duplicates()

    log(f"Total de {len(result)} registros únicos carregados para {date_str}")

    return result


def compara_dataframes(df_hoje: pd.DataFrame, df_ontem: pd.DataFrame) -> pd.DataFrame:
    """
    Compara dois DataFrames e retorna apenas os registros novos ou alterados.

    A comparação é feita em todas as colunas para identificar mudanças.

    Args:
        df_hoje: DataFrame com dados de hoje
        df_ontem: DataFrame com dados de ontem

    Returns:
        pd.DataFrame: Registros que são novos ou foram alterados
    """
    # Garante que as colunas estejam na mesma ordem
    common_columns = [col for col in df_hoje.columns if col in df_ontem.columns]

    if not common_columns:
        log("Nenhuma coluna em comum entre hoje e ontem - retornando todos os registros")
        return df_hoje

    df_hoje = df_hoje[common_columns]
    df_ontem = df_ontem[common_columns]

    # Faz merge para identificar diferenças
    merged = df_hoje.merge(df_ontem, on=common_columns, how="left", indicator=True)

    # Filtra apenas registros que não existiam ontem (left_only)
    new_records = merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)

    return new_records


def extract_stu_data(source: SourceTable, timestamp: datetime) -> pd.DataFrame:
    """
    Extrai dados do STU a partir dos arquivos do Airbyte no GCS.

    Compara os dados de hoje com os de ontem e retorna apenas os registros
    que são novos ou foram alterados.

    Args:
        source: Objeto SourceTable com configurações da tabela
        timestamp: Timestamp de referência da captura

    Returns:
        List[dict]: Lista de registros novos/alterados no formato JSON
    """
    log(f"Iniciando extração do STU para tabela {source.table_id}")

    st = Storage(
        env=source.env,
        dataset_id=source.dataset_id,
        table_id=source.table_id,
        bucket_names=STU_PRIVATE_BUCKET_NAMES,
    )

    hoje = timestamp
    ontem = hoje - timedelta(days=1)

    hoje_str = hoje.strftime("%Y_%m_%d")
    ontem_str = ontem.strftime("%Y_%m_%d")

    log(f"Buscando dados de hoje ({hoje_str}) e ontem ({ontem_str})")

    # Lista todos os blobs no prefixo
    prefix = f"ingestion/source_{STU_SOURCE_NAME}/stu_{source.table_id}/"
    blobs = list(st.bucket.list_blobs(prefix=prefix))

    log(f"Total de {len(blobs)} blobs encontrados no prefixo {prefix}")

    # Carrega dados de hoje
    df_hoje = processa_dados(blobs, hoje_str)
    log(f"Registros de hoje: {len(df_hoje)}")

    if df_hoje.empty:
        log("Nenhum dado encontrado para hoje")
        return []

    # Carrega dados de ontem para comparação
    df_ontem = processa_dados(blobs, ontem_str)
    log(f"Registros de ontem: {len(df_ontem)}")

    # Identifica novos registros ou alterados
    if df_ontem.empty:
        log("Sem dados de ontem - todos os registros são considerados novos")
        new_records = df_hoje
    else:
        log("Comparando dados de hoje vs ontem...")
        new_records = compara_dataframes(df_hoje, df_ontem)

    log(f"Total de registros novos/alterados: {len(new_records)}")

    return new_records
