# -*- coding: utf-8 -*-
"""
Tasks for gtfs
"""
import pytz
import os
import zipfile
from datetime import datetime

import openpyxl as xl
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants
from pipelines.migration.br_rj_riodejaneiro_gtfs.utils import (
    check_os_columns,
    check_os_columns_order,
    get_trips,
    get_board,
    check_os_filetype,
    download_controle_os_csv,
    download_file,
    download_xlsx,
    filter_valid_rows,
    processa_ordem_servico,
    processa_ordem_servico_trajeto_alternativo,
)
from pipelines.migration.utils import save_raw_local_func, get_secret
from pipelines.migration.tasks import format_send_discord_message

@task
def get_last_capture_os(dataset_id: str, mode: str = "prod") -> dict:
    """
    Retrieves the last captured OS for a given dataset ID and mode.

    Args:
        dataset_id (str): The ID of the dataset.
        mode (str, optional): The mode of operation. Defaults to "prod".

    Returns:
        dict: The last captured OS.

    """
    redis_client = get_redis_client()
    fetch_key = f"{dataset_id}.last_captured_os"
    if mode != "prod":
        fetch_key = f"{mode}.{fetch_key}"

    last_captured_os = redis_client.get(fetch_key)
    if last_captured_os is not None:
        last_captured_os = last_captured_os["last_captured_os"]

    #  verifica se last_capture_os tem formado dia/mes/ano_index
    if last_captured_os is not None:
        if "/" in last_captured_os:
            index = last_captured_os.split("_")[1]
            data = datetime.strptime(last_captured_os.split("_")[0], "%d/%m/%Y").strftime(
                "%Y-%m-%d"
            )
            last_captured_os = data + "_" + index

    log(f"Last captured os: {last_captured_os}")

    return last_captured_os


@task
def update_last_captured_os(dataset_id: str, data_index: str, mode: str = "prod") -> None:
    """
    Update the last captured operating system for a given dataset.

    Args:
        dataset_id (str): The ID of the dataset.
        data_index (str): The last captured operating system.
        mode (str, optional): The mode of operation. Defaults to "prod".

    Returns:
        None
    """
    redis_client = get_redis_client()
    fetch_key = f"{dataset_id}.last_captured_os"
    if mode != "prod":
        fetch_key = f"{mode}.{fetch_key}"
    last_captured_os = redis_client.get(fetch_key)
    #  verifica se last_capture_os tem formado dia/mes/ano_index e converte para ano-mes-dia_index
    if last_captured_os is not None:
        if "/" in last_captured_os:
            index = last_captured_os.split("_")[1]
            data = datetime.strptime(last_captured_os.split("_")[0], "%d/%m/%Y").strftime(
                "%Y-%m-%d"
            )
            last_captured_os = data + "_" + index
    # verifica se a ultima os capturada é maior que a nova
    if last_captured_os is not None:
        if last_captured_os["last_captured_os"] > data_index:
            return
    redis_client.set(fetch_key, {"last_captured_os": data_index})


@task(nout=4)
def get_os_info(last_captured_os: str = None, data_versao_gtfs: str = None) -> dict:
    """
    Retrieves information about the OS.

    Args:
        last_captured_os (str): The last captured OS data_index.

    Returns:
        tuple: A tuple containing the following elements:
            - flag_new_os (bool): Indicates whether a new OS was found.
            - data (dict): A dictionary containing the OS information.
            - data_index (str): The index of the captured OS.
            - inicio_vigencia_os (str): The start date of the captured OS.

    """
    df = download_controle_os_csv(constants.GTFS_CONTROLE_OS_URL.value)

    flag_new_os = False
    data = {"Início da Vigência da OS": None, "data_index": None}

    if df.empty:
        return flag_new_os, data, data["data_index"], data["Início da Vigência da OS"]

    df = filter_valid_rows(df)

    # converte "Início da Vigência da OS" de dd/mm/aaaa para aaaa-mm-dd
    df["Início da Vigência da OS"] = pd.to_datetime(
        df["Início da Vigência da OS"], format="%d/%m/%Y"
    ).dt.strftime("%Y-%m-%d")

    df["data_index"] = df["Início da Vigência da OS"].astype(str) + "_" + df["index"].astype(str)

    # Ordena por data e index
    df = df.sort_values(by=["data_index"], ascending=True)
    if data_versao_gtfs is not None:
        df = df.loc[(df["Início da Vigência da OS"] == data_versao_gtfs)]

    elif last_captured_os is None:
        last_captured_os = df["data_index"].max()
        df = df.loc[(df["data_index"] == last_captured_os)]

    else:
        # Filtra linhas onde 'data_index' é maior que o último capturado
        df = df.loc[(df["data_index"] > last_captured_os)]

    log(f"Os info: {df.head()}")
    if len(df) >= 1:
        log("Nova OS encontrada!")
        data = df.to_dict(orient="records")[0]  # Converte o DataFrame para um dicionário
        flag_new_os = True  # Se houver mais de uma OS, é uma nova OS

        log(f"OS selecionada: {data}")
    return flag_new_os, data, data["data_index"], data["Início da Vigência da OS"]


@task(nout=2)
def get_raw_drive_files(os_control, local_filepath: list, regular_sheet_index: int = None):
    """
    Downloads raw files from Google Drive and processes them.

    Args:
        os_control (dict): A dictionary containing information about the OS (Ordem de Serviço).
        local_filepath (list): A list of local file paths where the downloaded files will be saved.

    Returns:
        raw_filepaths (list): A list of file paths where the downloaded raw files are saved.
        primary_keys (list[list]): A list with the primary_keys for the tables.
    """

    raw_filepaths = []

    log(f"Baixando arquivos: {os_control}")

    # Autenticar usando o arquivo de credenciais
    credentials = service_account.Credentials.from_service_account_file(
        filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )

    # Criar o serviço da API Google Drive e Google Sheets
    drive_service = build("drive", "v3", credentials=credentials)

    # Baixa planilha de OS
    file_link = os_control["Link da OS"]
    file_bytes_os = download_xlsx(file_link=file_link, drive_service=drive_service)

    # Baixa GTFS
    file_link = os_control["Link do GTFS"]
    file_bytes_gtfs = download_file(file_link=file_link, drive_service=drive_service)

    # Salva os nomes das planilhas
    sheetnames = xl.load_workbook(file_bytes_os).sheetnames
    sheetnames = [name for name in sheetnames if "ANEXO" in name]
    log(f"tabs encontradas na planilha Controle OS: {sheetnames}")

    with zipfile.ZipFile(file_bytes_gtfs, "r") as zipped_file:
        for filename in list(constants.GTFS_TABLE_CAPTURE_PARAMS.value.keys()):
            if filename == "ordem_servico":
                processa_ordem_servico(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                    regular_sheet_index=regular_sheet_index,
                )
            elif filename == "ordem_servico_trajeto_alternativo":
                processa_ordem_servico_trajeto_alternativo(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                )

            else:
                # Processa arquivos do GTFS
                data = zipped_file.read(filename + ".txt")
                data = data.decode(encoding="utf-8")

                # encontra a partição correta
                local_file_path = list(filter(lambda x: filename + "/" in x, local_filepath))[0]

                raw_file_path = save_raw_local_func(
                    data=data, filepath=local_file_path, filetype="txt"
                )
                log(f"Saved file: {raw_file_path}")

                raw_filepaths.append(raw_file_path)

    return raw_filepaths, list(constants.GTFS_TABLE_CAPTURE_PARAMS.value.values())

### Validation

@task
def validate_gtfs_os(os_filepath, gtfs_filepath, os_initial_date, os_final_date):
    messages = []
    with open(gtfs_filepath, 'rb') as f:
        gtfs_file = f.read()
    # Check OS and GTFS files
    if os_filepath and gtfs_file:

        if not check_os_filetype(os_filepath):
            messages.append(
                ":warning: O nome do arquivo OS não é do tipo correto! Transforme o arquivo no formato .xlsx do Excel.")
            return
        
        os_sheets = pd.read_excel(os_filepath, None)

        if len(os_sheets) == 1:
            os_df = os_sheets.popitem()[1]
        else:
            messages.append(
                "O arquivo possui mais de uma aba, selecione a aba que contém os dados")
            

        viagens_cols = ["Viagens Dia Útil", "Viagens Sábado",
                        "Viagens Domingo", "Viagens Ponto Facultativo"]
        km_cols = ["Quilometragem Dia Útil", "Quilometragem Sábado",
                    "Quilometragem Domingo", "Quilometragem Ponto Facultativo"]


        if not check_os_columns(os_df):
            messages.append(
                ":warning: O arquivo OS não contém as colunas esperadas!")
            return

        for col in viagens_cols + km_cols:
            os_df[col] = (
                os_df[col].astype(str)
                .str.strip()
                .str.replace("—", "0")
                .str.replace(",", ".")
                .astype(float)
                .fillna(0)
                
            )
            os_df[col] = os_df[col].astype(float)

        
        if not check_os_columns_order(os_df):
            messages.append(
                f":warning: O arquivo OS contém as colunas esperadas, porém não segue a ordem esperada: {os_columns}")

        # Check dates

        if (os_initial_date is not None) and (os_final_date is not None):
            
            if os_initial_date > datetime.now().date():
                messages.append(
                    ":warning: ATENÇÃO: Você está subindo uma OS cuja operação já começou! Prossiga se é isso mesmo, senão revise as datas escolhidas."
                )
            
            trips_agg = get_trips(gtfs_file)
            quadro = get_board(os_df)
            quadro_merged = quadro.merge(trips_agg, on='servico', how='left')
            if len(quadro_merged[(quadro_merged["trip_id_ida"].isna()) & (quadro_merged["trip_id_volta"].isna())]) > 0:
                messages.append(
                    ":warning: ATENÇÃO: Existem trip_ids nulas"
                )

            if len(quadro_merged[((quadro_merged["partidas_ida_du"] > 0) & (quadro_merged["trip_id_ida"].isna())) | 
                ((quadro_merged["partidas_volta_du"] > 0) & (quadro_merged["trip_id_volta"].isna()))].sort_values('servico')) > 0:
                messages.append(
                    ":warning: ATENÇÃO: Existem viagens com ida e volta que possuem trip_ids nulas"
                )

            if len(quadro_merged[((quadro_merged["extensao_ida"] == 0) & ~(quadro_merged["trip_id_ida"].isna())) | 
                ((quadro_merged["extensao_volta"] == 0) & ~(quadro_merged["trip_id_volta"].isna()))]) > 0:
                messages.append(
                    ":warning: ATENÇÃO: Existem viagens programadas sem extensão definida"
                )
            # Check data
            # TODO: Partidas x Extensão, Serviços OS x GTFS (routes, trips, shapes), Extensão OS x GTFS"

            # # Numero de servicos por consorcio
            # tb = pd.DataFrame(os_df.groupby(
            #     "Consórcio")["Serviço"].count())
            # tb.loc["Total"] = tb.sum()
            # st.table(tb)

            # # Numero de viagens por consorcio
            # tb = pd.DataFrame(os_df.groupby(
            #     "Consórcio")[viagens_cols].sum())
            # tb.loc["Total"] = tb.sum()
            # st.table(tb.style.format("{:.1f}"))

            # # Numero de KM por consorcio
            # tb = pd.DataFrame(os_df.groupby(
            #     "Consórcio")[km_cols].sum())
            # tb.loc["Total"] = tb.sum()
            # st.table(tb.style.format("{:.3f}"))
    return messages

@task
def send_check_report(messages=None):
    webhook_url = get_secret("gtfs_check_webhook")['url']
    base_msg = f'Reporte de validação GTFS e OS {datetime.now(tz=pytz.timezone("America/Sao_Paulo")).date().isoformat()}'
    if not messages:
        msg = "GTFS e OS passaram na validação!"
        send_msg = f"{base_msg}\n{msg}"
        format_send_discord_message([send_msg], webhook_url)
        return
    send_msgs = [base_msg] + messages
    format_send_discord_message(send_msgs, webhook_url)