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


def remove_arquivos(blobs: List, date: datetime, days: int = 30) -> None:
    """
    Remove arquivos do bucket.

    Args:
        blobs: Lista de blobs disponíveis no bucket
        date: Data de referência
        days: Número de dias a subtrair da data de referência
    """
    files_to_delete = []
    cutoff_date = date - timedelta(days=days)
    cutoff_date_str = cutoff_date.strftime("%Y_%m_%d")

    for blob in blobs:
        try:
            filename = blob.name.split("/")[-1]

            if cutoff_date_str in filename and filename.endswith(".csv"):
                files_to_delete.append(blob)

        except Exception as e:
            log(f"Erro ao processar arquivo {blob.name}: {str(e)}", level="warning")
            continue

    if files_to_delete:
        for blob in files_to_delete:
            try:
                log(f"Deletando arquivo: {blob.name}")
                blob.delete()
            except Exception as e:
                log(f"Erro ao deletar arquivo {blob.name}: {str(e)}", level="error")
        log(f"Deletados {len(files_to_delete)} arquivos com sucesso")
    else:
        log("Nenhum arquivo antigo encontrado para deletar")


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
                # Remove caracteres invisíveis
                data = data.replace({r"[\x00-\x1F\x7F]": ""}, regex=True)
                aux_df.append(data)
            else:
                log(f"Arquivo {filename} não contém coluna _airbyte_data")

        except Exception as e:
            log(f"Erro ao processar arquivo {file.name}: {str(e)}", level="error")
            continue

    if not aux_df:
        return pd.DataFrame()

    result = pd.concat(aux_df, ignore_index=True)

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
    # Verifica se há novas colunas em df_hoje que não existem em df_ontem
    new_columns = set(df_hoje.columns) - set(df_ontem.columns)

    if new_columns:
        log(f"Novas colunas detectadas: {new_columns} - retornando todos os registros")
        return df_hoje

    # Garante que as colunas sejam as mesmas
    common_columns = [col for col in df_hoje.columns if col in df_ontem.columns]
    if not common_columns:
        log("Nenhuma coluna em comum entre hoje e ontem - retornando todos os registros")
        return df_hoje

    # Faz merge para identificar diferenças
    merged = df_hoje.merge(df_ontem, how="left", indicator=True)

    # Filtra apenas registros que não existiam ontem (left_only)
    new_records = merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)

    return new_records


def extract_stu_data(source: SourceTable, timestamp: datetime) -> pd.DataFrame:
    """
    Extrai dados do STU a partir dos arquivos do Airbyte no GCS.

    Compara os dados de hoje com o dia anterior mais recente que tenha dados
    e retorna apenas os registros que são novos ou foram alterados.

    Args:
        source: Objeto SourceTable com configurações da tabela
        timestamp: Timestamp de referência da captura

    Returns:
        pd.DataFrame: DataFrame com os registros novos/alterados
    """
    log(f"Iniciando extração do STU para tabela {source.table_id}")

    st = Storage(
        env=source.env,
        dataset_id=source.dataset_id,
        table_id=source.table_id,
        bucket_names=STU_PRIVATE_BUCKET_NAMES,
    )

    hoje = timestamp
    hoje_str = hoje.strftime("%Y_%m_%d")

    # Verifica se é primeira execução
    first_run = (
        source.first_timestamp is not None and timestamp.date() == source.first_timestamp.date()
    )

    log(f"Buscando dados de hoje ({hoje_str})")

    # Lista todos os blobs no prefixo
    prefix = f"ingestion/source_{STU_SOURCE_NAME}/stu_{source.table_id}/"
    blobs = list(st.bucket.list_blobs(prefix=prefix))

    log(f"Total de {len(blobs)} blobs encontrados no prefixo {prefix}")

    # Carrega dados de hoje
    df_hoje = processa_dados(blobs, hoje_str)
    log(f"Registros de hoje: {len(df_hoje)}")

    if df_hoje.empty:
        log("Nenhum dado encontrado para hoje")
        return pd.DataFrame()

    if first_run:
        return df_hoje

    # Busca dados do dia anterior mais recente com dados
    max_days_back = 30
    df_anterior = pd.DataFrame()

    for days_back in range(1, max_days_back + 1):
        data_anterior = hoje - timedelta(days=days_back)
        data_anterior_str = data_anterior.strftime("%Y_%m_%d")

        log(f"Buscando dados de {data_anterior_str} ({days_back} dia(s) atrás)")
        df_anterior = processa_dados(blobs, data_anterior_str)

        if not df_anterior.empty:
            log(f"Encontrados {len(df_anterior)} registros em {data_anterior_str}")
            break

    # Identifica novos registros ou alterados
    if df_anterior.empty:
        log(
            f"Sem dados nos últimos {max_days_back} dias - todos os registros são considerados novos"  # noqa
        )
        new_records = df_hoje
    else:
        log("Comparando dados de hoje vs dia anterior encontrado...")
        new_records = compara_dataframes(df_hoje, df_anterior)

    log(f"Total de registros novos/alterados: {len(new_records)}")

    # Remove arquivos antigos (30 dias)
    log("Iniciando limpeza de arquivos antigos...")
    remove_arquivos(blobs=blobs, date=hoje, days=30)

    return new_records
