# -*- coding: utf-8 -*-
import base64
import io
import os
import zipfile
from datetime import datetime
from os import environ

import openpyxl as xl
import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.constants import constants
from pipelines.utils.backup.utils import save_raw_local_func
from pipelines.utils.secret import get_secret


@task
def get_last_capture_os(dataset_id: str, mode: str = "prod") -> dict:
    redis_client = get_redis_client()
    fetch_key = f"{dataset_id}.last_captured_os"
    if mode != "prod":
        fetch_key = f"{mode}.{fetch_key}"

    last_captured_os = redis_client.get(fetch_key)

    log(f"Last captured os: {last_captured_os}")

    # last_captured_os = "MTR-DES-2024/30822"
    ### teste sem redis ###
    return last_captured_os


@task
def update_last_captured_os(dataset_id: str, despacho: str, mode: str = "prod") -> None:
    redis_client = get_redis_client()
    fetch_key = f"{dataset_id}.last_captured_os"
    if mode != "prod":
        fetch_key = f"{mode}.{fetch_key}"

    redis_client.set(fetch_key, {"last_captured_os": despacho})


@task(nout=4)
def get_os_info(last_captured_os: str) -> dict:

    df = download_controle_os_csv(constants.GTFS_CONTROLE_OS_URL.value)
    flag_new_os = False
    data = {"Início da Vigência da OS": None, "ano_id_despacho": None}

    if df.empty:
        return flag_new_os, data, data["ano_id_despacho"], data["Início da Vigência da OS"]

    # df = filter_valid_rows(df)

    log(f"Os info: {df.head()}")

    df["ano_id_despacho"] = df["Despacho"].apply(lambda x: x.split("-")[-1])
    if last_captured_os is None:
        last_captured_os = df["ano_id_despacho"].max()
    else:

        # Filtra linhas onde 'Despacho' é maior ou igual que o último capturado
        df = df.loc[(df["ano_id_despacho"] > last_captured_os)]

        # Ordena por despacho
        df = df.sort_values(by=["ano_id_despacho"])

    # Mantem apenas colunas necessarias
    df = df[
        [
            "ano_id_despacho",
            "Início da Vigência da OS",
            "Arquivo OS",
            "Arquivo GTFS",
            "Link da OS",
            "Link do GTFS",
        ]
    ]

    if len(df) >= 1:
        log("Nova OS encontrada!")
        data = df.to_dict(orient="records")[0]  # Converte o DataFrame para um dicionário
        flag_new_os = True  # Se houver mais de uma OS, é uma nova OS
        # converte "Início da Vigência da OS" de dd/mm/aaaa para aaaa-mm-dd
        data["Início da Vigência da OS"] = datetime.strptime(
            data["Início da Vigência da OS"], "%d/%m/%Y"
        ).strftime("%Y-%m-%d")

    return flag_new_os, data, data["ano_id_despacho"], data["Início da Vigência da OS"]


## mover para utils ##
def download_controle_os_csv(url):

    response = requests.get(url=url, timeout=constants.MAX_TIMEOUT_SECONDS.value)
    response.raise_for_status()  # Verifica se houve algum erro na requisição
    response.encoding = "utf-8"
    # Carrega o conteúdo da resposta em um DataFrame
    df = pd.read_csv(io.StringIO(response.text))

    ## separar em outra função ##
    if not df.empty:
        # Remove linhas com valores nulos
        df.dropna(how="all", inplace=True)

        # Remove linhas onde 'Fim da Vigência da OS' == 'Sem Vigência'
        df = df[df["Fim da Vigência da OS"] != "Sem Vigência"]

        # Remove linhas onde 'Submeter mudanças para Dados' == False
        df = df[df["Submeter mudanças para Dados"] == True]

        # Remove linhas onde 'Arquivo OS' e 'Arquivo GTFS' são nulos
        df = df[~df["Arquivo OS"].isnull()]
        df = df[~df["Arquivo GTFS"].isnull()]
        df = df[~df["Link da OS"].isnull()]
        df = df[~df["Link do GTFS"].isnull()]

    log(f"Download concluído! Dados:\n{df.head()}")
    return df


## Mover para utils ##
def convert_to_float(value):
    if "," in value:
        value = value.replace(".", "").replace(",", ".").strip()
    return float(value)


## refatorar em funções menores ##
@task(nout=2)
def get_raw_drive_files(os_control, local_filepath: list):
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
    # log(get_secret("GOOGLE_APPLICATION_CREDENTIALS"))
    credentials_info = base64.b64decode(get_secret(secret_name="BASEDOSDADOS_CREDENTIALS_PROD"))
    log(credentials_info)
    # Autenticar usando o arquivo de credenciais
    credentials = service_account.Credentials.from_service_account_info(
        # filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        info=credentials_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )

    # Criar o serviço da API Google Drive e Google Sheets
    drive_service = build("drive", "v3", credentials=credentials)

    file_link = os_control["Link da OS"]

    file_id = file_link.split("/")[-2]

    request = drive_service.files().export_media(  # pylint: disable=E1101
        fileId=file_id,
        mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    file_bytes = io.BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    # Processar a planilha
    file_bytes.seek(0)
    wb = xl.load_workbook(file_bytes)
    sheetnames = wb.sheetnames
    quadro_geral = pd.DataFrame()

    # conta quantos sheets tem no arquivo sem o nome de Anexo II
    sheets_range = len(sheetnames) - len([x for x in sheetnames if "ANEXO II" in x])

    for i in range(0, sheets_range):
        log(f"########## {sheetnames[i]} ##########")
        quadro = pd.read_excel(file_bytes, sheet_name=sheetnames[i], dtype=object)

        columns = {
            "Serviço": "servico",
            "Vista": "vista",
            "Consórcio": "consorcio",
            "Extensão de Ida": "extensao_ida",
            "Extensão de Volta": "extensao_volta",
            "Horário Inicial": "horario_inicio",
            "Horário\nInicial": "horario_inicio",
            "Horário Fim": "horario_fim",
            "Horário\nFim": "horario_fim",
            "Partidas Ida Dia Útil": "partidas_ida_du",
            "Partidas Volta Dia Útil": "partidas_volta_du",
            "Viagens Dia Útil": "viagens_du",
            "Quilometragem Dia Útil": "km_dia_util",
            "Partidas Ida Sábado": "partidas_ida_sabado",
            "Partidas Volta Sábado": "partidas_volta_sabado",
            "Viagens Sábado": "viagens_sabado",
            "Quilometragem Sábado": "km_sabado",
            "Partidas Ida Domingo": "partidas_ida_domingo",
            "Partidas Volta Domingo": "partidas_volta_domingo",
            "Viagens Domingo": "viagens_domingo",
            "Quilometragem Domingo": "km_domingo",
            "Partidas Ida Ponto Facultativo": "partidas_ida_pf",
            "Partidas Volta Ponto Facultativo": "partidas_volta_pf",
            "Viagens Ponto Facultativo": "viagens_pf",
            "Quilometragem Ponto Facultativo": "km_pf",
        }

        quadro = quadro.rename(columns=columns)

        quadro["servico"] = quadro["servico"].astype(str)
        quadro["servico"] = quadro["servico"].str.extract(r"([A-Z]+)", expand=False).fillna(
            ""
        ) + quadro["servico"].str.extract(r"([0-9]+)", expand=False).fillna("")

        quadro = quadro[list(set(columns.values()))]
        quadro = quadro.replace("—", 0)
        quadro = quadro.reindex(columns=list(set(columns.values())))

        hora_cols = [coluna for coluna in quadro.columns if "horario" in coluna]
        quadro[hora_cols] = quadro[hora_cols].astype(str)

        for hora_col in hora_cols:
            quadro[hora_col] = quadro[hora_col].apply(lambda x: x.split(" ")[1] if " " in x else x)

        cols = [
            coluna
            for coluna in quadro.columns
            if "km" in coluna or "viagens" in coluna or "partida" in coluna
        ]

        for col in cols:
            quadro[col] = quadro[col].astype(str).apply(convert_to_float).astype(float).fillna(0)

        extensao_cols = ["extensao_ida", "extensao_volta"]
        quadro[extensao_cols] = quadro[extensao_cols].astype(str)
        quadro[extensao_cols] = quadro[extensao_cols].apply(pd.to_numeric)

        quadro["extensao_ida"] = quadro["extensao_ida"] / 1000
        quadro["extensao_volta"] = quadro["extensao_volta"] / 1000

        if i == 0:
            quadro["tipo_os"] = "Regular"

        quadro_geral = pd.concat([quadro_geral, quadro])

    # Verificações
    columns_in_dataframe = set(quadro_geral.columns)
    columns_in_values = set(list(columns.values()) + ["tipo_os"])

    all_columns_present = columns_in_dataframe.issubset(columns_in_values)
    no_duplicate_columns = len(columns_in_dataframe) == len(quadro_geral.columns)
    missing_columns = columns_in_values - columns_in_dataframe

    log(
        f"All columns present: {all_columns_present}/"
        f"No duplicate columns: {no_duplicate_columns}/"
        f"Missing columns: {missing_columns}"
    )

    if not all_columns_present or not no_duplicate_columns:
        raise Exception("Missing or duplicated columns in ordem_servico")

    quadro_test = quadro_geral.copy()
    quadro_test["km_test"] = round(
        (quadro_geral["partidas_volta_du"] * quadro_geral["extensao_volta"])
        + (quadro_geral["partidas_ida_du"] * quadro_geral["extensao_ida"]),
        2,
    )
    quadro_test["dif"] = quadro_test["km_test"] - quadro_test["km_dia_util"]

    if not (
        round(abs(quadro_test["dif"].max()), 2) <= 0.01
        and round(abs(quadro_test["dif"].min()), 2) <= 0.01
    ):
        raise Exception("failed to validate km_test and km_dia_util")

    local_file_path = list(filter(lambda x: "ordem_servico" in x, local_filepath))[0]
    quadro_geral_csv = quadro_geral.to_csv(index=False)
    raw_file_path = save_raw_local_func(
        data=quadro_geral_csv, filepath=local_file_path, filetype="csv"
    )
    log(f"Saved file: {raw_file_path}")

    raw_filepaths.append(raw_file_path)

    # Pre-tratamento para "Trajeto Alternativo"
    sheet = -1
    log(f"########## {sheetnames[sheet]} ##########")

    ordem_servico_trajeto_alternativo = pd.read_excel(
        file_bytes, sheet_name=sheetnames[sheet], dtype=object
    )

    alt_columns = {
        "Serviço": "servico",
        "Vista": "vista",
        "Consórcio": "consorcio",
        "Extensão de Ida": "extensao_ida",
        "Extensão\nde Ida": "extensao_ida",
        "Extensão de Volta": "extensao_volta",
        "Extensão\nde Volta": "extensao_volta",
        "Evento": "evento",
        "Horário Inicial Interdição": "inicio_periodo",
        "Horário Final Interdição": "fim_periodo",
        "Descrição": "descricao",
        "Ativação": "ativacao",
    }

    ordem_servico_trajeto_alternativo = ordem_servico_trajeto_alternativo.rename(
        columns=alt_columns
    )

    columns_in_dataframe = set(ordem_servico_trajeto_alternativo.columns)
    columns_in_values = set(list(alt_columns.values()))

    all_columns_present = columns_in_dataframe.issubset(columns_in_values)
    no_duplicate_columns = len(columns_in_dataframe) == len(
        ordem_servico_trajeto_alternativo.columns
    )
    missing_columns = columns_in_values - columns_in_dataframe

    log(
        f"All columns present: {all_columns_present}/"
        f"No duplicate columns: {no_duplicate_columns}/"
        f"Missing columns: {missing_columns}"
    )

    if not all_columns_present or not no_duplicate_columns:
        raise Exception("Missing or duplicated columns in ordem_servico_trajeto_alternativo")

    local_file_path = list(
        filter(lambda x: "ordem_servico_trajeto_alternativo" in x, local_filepath)
    )[0]
    ordem_servico_trajeto_alternativo_csv = ordem_servico_trajeto_alternativo.to_csv(index=False)
    raw_file_path = save_raw_local_func(
        data=ordem_servico_trajeto_alternativo_csv,
        filepath=local_file_path,
        filetype="csv",
    )
    log(f"Saved file: {raw_file_path}")

    raw_filepaths.append(raw_file_path)

    file_link = os_control["Link do GTFS"]
    file_id = file_link.split("/")[-2]

    request = drive_service.files().get_media(fileId=file_id)  # pylint: disable=E1101
    file_bytes = io.BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    with zipfile.ZipFile(file_bytes, "r") as zipped_file:
        for filename in constants.GTFS_TABLE_CAPTURE_PARAMS.value[2:].keys():

            data = zipped_file.read(filename + ".txt")
            data = data.decode(encoding="utf-8")

            # encontra a partição correta
            local_file_path = list(filter(lambda x: filename in x, local_filepath))[0]

            raw_file_path = save_raw_local_func(data=data, filepath=local_file_path, filetype="txt")
            log(f"Saved file: {raw_file_path}")

            raw_filepaths.append(raw_file_path)

    return raw_filepaths, constants.GTFS_TABLE_CAPTURE_PARAMS.value.values()
