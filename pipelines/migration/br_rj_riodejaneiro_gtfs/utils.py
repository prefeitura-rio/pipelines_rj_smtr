# -*- coding: utf-8 -*-
"""
Utils for gtfs
"""
from datetime import datetime
import io

import openpyxl as xl
import pandas as pd
import re
import requests
from googleapiclient.http import MediaIoBaseDownload
from zipfile import ZipFile, ZIP_DEFLATED

from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.migration.utils import save_raw_local_func


def filter_valid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the valid rows in a DataFrame based on specific conditions.

    Args:
        df (pd.DataFrame): The input DataFrame to filter.

    Returns:
        pd.DataFrame: The filtered DataFrame.

    """
    # salva index atual em uma coluna
    df["index"] = df.index
    # Remove linhas com valores nulos
    df.dropna(how="all", inplace=True)

    # Remove linhas onde 'Fim da Vigência da OS' == 'Sem Vigência'
    df = df[df["Fim da Vigência da OS"] != "Sem Vigência"]

    # Remove linhas onde 'Submeter mudanças para Dados' == False
    df = df[df["Submeter mudanças para Dados"] == True]  # noqa

    # Remove linhas onde 'Arquivo OS' e 'Arquivo GTFS' são nulos
    df = df[~df["Início da Vigência da OS"].isnull()]
    df = df[~df["Arquivo OS"].isnull()]
    df = df[~df["Arquivo GTFS"].isnull()]
    df = df[~df["Link da OS"].isnull()]
    df = df[~df["Link do GTFS"].isnull()]
    return df


def download_controle_os_csv(url):
    """
    Downloads a CSV file from the given URL and returns its content as a pandas DataFrame.

    Args:
        url (str): The URL of the CSV file to download.

    Returns:
        pandas.DataFrame: The content of the downloaded CSV file as a DataFrame.

    """

    response = requests.get(url=url, timeout=constants.MAX_TIMEOUT_SECONDS.value)
    response.raise_for_status()  # Verifica se houve algum erro na requisição
    response.encoding = "utf-8"
    # Carrega o conteúdo da resposta em um DataFrame
    df = pd.read_csv(io.StringIO(response.text))

    log(f"Download concluído! Dados:\n{df.head()}")
    return df


def convert_to_float(value):
    """
    Converts a string value to a float.

    Args:
        value (str): The string value to be converted.

    Returns:
        float: The converted float value.
    """
    if "," in value:
        value = value.replace(".", "").replace(",", ".").strip()
    return float(value)


def processa_OS(file_link: str, drive_service, local_filepath: list, raw_filepaths: list):
    """
    Process the OS data from an Excel file.

    Args:
        file_link (str): The link to the Excel file.
        drive_service: The drive service object for downloading the file.
        local_filepath (list): The list of local file paths.
        raw_filepaths (list): The list of raw file paths.

    Returns:
        None
    """

    file_bytes = download_xlsx(file_link=file_link, drive_service=drive_service)

    # Processar a planilha
    wb = xl.load_workbook(file_bytes)
    sheetnames = wb.sheetnames

    processa_ordem_servico(
        sheetnames=sheetnames,
        file_bytes=file_bytes,
        local_filepath=local_filepath,
        raw_filepaths=raw_filepaths,
    )

    processa_ordem_servico_trajeto_alternativo(
        sheetnames=sheetnames,
        file_bytes=file_bytes,
        local_filepath=local_filepath,
        raw_filepaths=raw_filepaths,
    )


def download_xlsx(file_link, drive_service):
    """
    Downloads an XLSX file from Google Drive.

    Args:
        file_link (str): The link to the file in Google Drive.
        drive_service: The Google Drive service object.

    Returns:
        io.BytesIO: The downloaded XLSX file as a BytesIO object.
    """
    file_id = file_link.split("/")[-2]

    file = drive_service.files().get(fileId=file_id).execute()  # pylint: disable=E1101
    mime_type = file.get("mimeType")

    if "google-apps" in mime_type:
        request = drive_service.files().export(  # pylint: disable=E1101
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        request = drive_service.files().get_media(fileId=file_id)  # pylint: disable=E1101

    file_bytes = io.BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_bytes.seek(0)
    return file_bytes


def normalizar_horario(horario):
    """
    Normalizes the given time string.

    If the time string contains the word "day", it splits the string into days and time.
    It converts the days, hours, minutes, and seconds into total hours and returns
    the normalized time string.

    If the time string does not contain the word "day", it returns the time string as is.

    Args:
        horario (str): The time string to be normalized.

    Returns:
        str: The normalized time string.

    """
    if "day" in horario:
        days, time = horario.split(", ")
        days = int(days.split(" ")[0])
        hours, minutes, seconds = map(int, time.split(":"))
        total_hours = days * 24 + hours
        return f"{total_hours:02}:{minutes:02}:{seconds:02}"
    else:
        return horario.split(" ")[1] if " " in horario else horario


def processa_ordem_servico(
    sheetnames, file_bytes, local_filepath, raw_filepaths, regular_sheet_index=None
):
    """
    Process the OS data from an Excel file.

    Args:
        sheetnames (list): List of sheet names in the Excel file.
        file_bytes (bytes): The Excel file in bytes format.
        local_filepath (str): The local file path where the processed data will be saved.
        raw_filepaths (list): List of raw file paths.
        regular_sheet_index (int, optional): The index of the regular sheet. Defaults to 0.

    Raises:
        Exception: If there are more than 2 tabs in the file or if there are missing or
        duplicated columns in the order of service data.
        Exception: If the validation of 'km_test' and 'km_dia_util' fails.

    Returns:
        None
    """

    if len(sheetnames) != 2 and regular_sheet_index is None:
        raise Exception("More than 2 tabs in the file. Please specify the regular sheet index.")

    if regular_sheet_index is None:
        regular_sheet_index = 0

    sheets_range = len(sheetnames) - len([x for x in sheetnames if "ANEXO II" in x])
    quadro_geral = pd.DataFrame()

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
            quadro[hora_col] = quadro[hora_col].apply(normalizar_horario)

        cols = [
            coluna
            for coluna in quadro.columns
            if "km" in coluna or "viagens" in coluna or "partida" in coluna
        ]

        for col in cols:
            quadro[col] = quadro[col].astype(str).apply(convert_to_float).astype(float).fillna(0)

        extensao_cols = ["extensao_ida", "extensao_volta"]
        quadro[extensao_cols] = quadro[extensao_cols].astype(str)
        for col in extensao_cols:
            quadro[col] = quadro[col].str.replace(".", "", regex=False)
        quadro[extensao_cols] = quadro[extensao_cols].apply(pd.to_numeric)

        quadro["extensao_ida"] = quadro["extensao_ida"] / 1000
        quadro["extensao_volta"] = quadro["extensao_volta"] / 1000

        if i == regular_sheet_index:
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


def processa_ordem_servico_trajeto_alternativo(
    sheetnames, file_bytes, local_filepath, raw_filepaths
):
    """
    Process 'Trajetos Alternativos' from an Excel file.

    Args:
        sheetnames (list): List of sheet names in the Excel file.
        file_bytes (bytes): Bytes of the Excel file.
        local_filepath (str): Local file path.
        raw_filepaths (list): List of raw file paths.

    Returns:
        None

    Raises:
        Exception: If there are missing or duplicated columns in 'Trajetos Alternativos'.
    """
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


def download_file(file_link, drive_service):
    """
    Downloads a file from Google Drive.

    Args:
        file_link (str): The link to the file in Google Drive.
        drive_service: The Google Drive service object.

    Returns:
        io.BytesIO: The downloaded file as a BytesIO object.
    """
    file_id = file_link.split("/")[-2]

    request = drive_service.files().get_media(fileId=file_id)  # pylint: disable=E1101
    file_bytes = io.BytesIO()
    downloader = MediaIoBaseDownload(file_bytes, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return file_bytes

### Validation

def read_stream(stream: bytes) -> pd.DataFrame:
    file_text = str(stream,'utf-8')    
    string_buffer = io.StringIO(file_text) 
    return pd.read_csv(string_buffer)

def get_shapes(shapes: pd.DataFrame) -> pd.DataFrame:
    
    #Identifica pontos finais
    max_pt_shapes = shapes.groupby("shape_id").shape_pt_sequence.idxmax()

    #Identifica pontos iniciais
    min_pt_shapes = shapes.groupby("shape_id").shape_pt_sequence.idxmin()

    #Realiza merge entre dataframes
    shapes_min_max = shapes.loc[max_pt_shapes].merge(shapes.loc[min_pt_shapes], on='shape_id', how='left', suffixes=('_max', '_min'))

    #Identifica shapes circulares (mesma coordenada de início e término)
    #shapes_min_max['flag_circular'] = (shapes_min_max['shape_pt_lat_max'] == shapes_min_max['shape_pt_lat_min']) & (shapes_min_max['shape_pt_lon_max'] == shapes_min_max['shape_pt_lon_min'])
    # Arredondamento para 4ª casa decimal - Caso 651 e 652
    shapes_min_max['flag_circular'] = (
        (round(shapes_min_max['shape_pt_lat_max'],4) == round(shapes_min_max['shape_pt_lat_min'],4)) 
        & (round(shapes_min_max['shape_pt_lon_max'],4) == round(shapes_min_max['shape_pt_lon_min'],4))
    )
    
    # filtra shapes não circulares
    shapes_final = shapes[~shapes.shape_id.isin(shapes_min_max[shapes_min_max['flag_circular'] == True]['shape_id'].to_list())]

    # filtra shapes circulares
    shapes_circulares = shapes[shapes.shape_id.isin(shapes_min_max[shapes_min_max['flag_circular'] == True]['shape_id'].to_list())]

    # identifica metade do trajeto circular
    shapes_c_breakpoint = round(shapes_circulares.groupby("shape_id").shape_pt_sequence.max()/2, 0).to_dict()

    # separa metades do trajeto em ida + volta 
    shapes_circulares = pd.DataFrame()

    for idx in shapes_c_breakpoint:
        aux = shapes[shapes.shape_id == idx]

        shapes_final = pd.concat([shapes_final, aux])

        aux.loc[aux.shape_pt_sequence <= shapes_c_breakpoint[idx], "shape_id"] = f"{idx}_0"
        aux.loc[aux.shape_pt_sequence > shapes_c_breakpoint[idx], "shape_id"] = f"{idx}_1"
        aux["shape_pt_sequence"] = aux.groupby("shape_id").cumcount()

        # adiciona ida + volta na tabela final
        shapes_final = pd.concat([shapes_final, aux])
    return shapes_final

def get_trips(file: bytes) -> pd.DataFrame:

    # Descompacta o arquivo zip direto na memoria
    input_zip = ZipFile(io.BufferedRWPairBytesIO(file), 'r')
    files: dict[str, bytes] = {name: input_zip.read(name) for name in input_zip.namelist()}
    trips = read_stream(files['trips.txt'])
    agency = read_stream(files['agency.txt'])
    routes = read_stream(files['routes.txt'])
    shapes = read_stream(files['shapes.txt'])
    shapes_final = get_shapes(shapes)
    
    trips = trips.merge(routes, how='left', on='route_id').merge(agency, how='left', on='agency_id')
    
    trips_qh = trips.copy()
    # Serão considerados os trip_id de sábado para as linhas abaixo
    linhas_sab = ["SP601"]
    trips_qh = trips_qh.sort_values(by=["service_id"], ascending=False)
    trips_qh = trips_qh[((~(trips_qh['trip_short_name'].isin(linhas_sab)) &
                          (trips_qh['service_id'].str.startswith(("U_R", "U_O"), na=False))) | 
                         ((trips_qh['trip_short_name'].isin(linhas_sab)) & 
                          (trips_qh['service_id'].str.startswith(("S_R", "S_O"), na=False))))]
    trips_qh = (trips_qh[trips_qh['shape_id'].isin(list(set(shapes_final['shape_id'].to_list())))]
            .sort_values(['trip_short_name', 'service_id', 'shape_id', 'direction_id'])
            .drop_duplicates(['trip_short_name', 'direction_id']))
    
    trips_agg = (pd.pivot_table(trips_qh, values='trip_id', index = ['trip_short_name'], 
                            columns='direction_id', aggfunc='first')
             .rename_axis(None, axis=1)
             .reset_index()
             .rename(columns={'trip_short_name': 'servico', 0: 'trip_id_ida', 1: 'trip_id_volta'})
             [['servico', 'trip_id_ida', 'trip_id_volta']]
             .sort_values(by=['servico']))
    return trips_agg

def get_board(quadro: pd.DataFrame):
    columns = {'Serviço': 'servico',
           'Vista': 'vista',
           'Consórcio': 'consorcio',
           'Consorcio': 'consorcio',
           'Horário Inicial': 'horario_inicio',
           'Horário Início': 'horario_inicio',
           'Horário início': 'horario_inicio',
           'Horário Inicial Dia Útil': 'horario_inicio',
           'Horário Fim Dia Útil': 'horario_fim',
           'Horário Fim': 'horario_fim',
           'Horário fim': 'horario_fim',
           'Partidas Ida Dia Útil': 'partidas_ida_du',
           'Partidas ida dia útil': 'partidas_ida_du',
           'Partidas Ida\n(DU)': 'partidas_ida_du',
           'Partidas Volta Dia Útil': 'partidas_volta_du',
           'Partidas volta dia útil': 'partidas_volta_du',
           'Partidas Volta\n(DU)': 'partidas_volta_du',
           'Extensão de Ida': 'extensao_ida',
           'Extensão de ida': 'extensao_ida',
           'Ext.\nIda': 'extensao_ida',
           'Extensão de Volta': 'extensao_volta',
           'Extensão de volta': 'extensao_volta',
           'Ext.\nVolta': 'extensao_volta',
           'Viagens Dia Útil': 'viagens_du',
           'Viagens dia útil': 'viagens_du',
           'Viagens\n(DU)': 'viagens_du',
           'Quilometragem Dia Útil': 'km_dia_util',
           'Quilometragem dia útil': 'km_dia_util',
           'KM\n(DU)': 'km_dia_util',
           'Quilometragem Sábado': 'km_sabado',
           'Quilometragem sábado': 'km_sabado',
           'KM\n(SAB)': 'km_sabado',
           'Quilometragem Domingo': 'km_domingo',
           'Quilometragem domingo': 'km_domingo',
           'KM\n(DOM)': 'km_domingo',
           'Partida Ida Ponto Facultativo': 'partidas_ida_pf',
           'Partidas Ida Ponto Facultativo': 'partidas_ida_pf',
           'Partidas Ida\n(FAC)': 'partidas_ida_pf',
           'Partida Volta Ponto Facultativo': 'partidas_volta_pf',
           'Partidas Volta Ponto Facultativo': 'partidas_volta_pf',
           'Partidas Volta\n(FAC)': 'partidas_volta_pf',   
           'Viagens Ponto Facultativo': 'viagens_pf',
           'Viagens\n(FAC)': 'viagens_pf',
           'Quilometragem Ponto Facultativo': 'km_pf',
           'KM\n(FAC)': 'km_pf'}

    quadro = quadro.rename(columns = columns)

    # corrige nome do servico
    quadro["servico"] = quadro["servico"].astype(str)
    quadro["servico"] = quadro["servico"].str.extract(r"([A-Z]+)").fillna("") + quadro[
        "servico"
    ].str.extract(r"([0-9]+)")
    quadro = quadro[list(set(columns.values()))]

    quadro = quadro.replace("—", 0)

    #Ajusta colunas numéricas
    numeric_cols = quadro.columns.difference(["servico", "vista", "consorcio", "horario_inicio", "horario_fim", "extensao_ida", "extensao_volta"]).to_list()
    quadro[numeric_cols] = quadro[numeric_cols].astype(str)
    quadro[numeric_cols] = quadro[numeric_cols].apply(lambda x: x.str.replace(".", ""))
    quadro[numeric_cols] = quadro[numeric_cols].apply(lambda x: x.str.replace(",", "."))
    quadro[numeric_cols] = quadro[numeric_cols].apply(pd.to_numeric)

    extensao_cols = ["extensao_ida", "extensao_volta"]
    quadro[extensao_cols] = quadro[extensao_cols].astype(str)
    quadro[extensao_cols] = quadro[extensao_cols].apply(lambda x: x.str.replace(",00", ""))
    quadro[extensao_cols] = quadro[extensao_cols].apply(pd.to_numeric)

    quadro["extensao_ida"] = quadro["extensao_ida"]/1000
    quadro["extensao_volta"] = quadro["extensao_volta"]/1000

    # Ajusta colunas com hora
    hora_cols = [coluna for coluna in quadro.columns if "horario" in coluna]
    quadro[hora_cols] = quadro[hora_cols].astype(str)

    for hora_col in hora_cols:
        quadro[hora_col] = quadro[hora_col].apply(lambda x: x.split(" ")[1] if " " in x else x)

    # Ajusta colunas de km
    hora_cols = [coluna for coluna in quadro.columns if "km" in coluna]

    for hora_col in hora_cols:
        quadro[hora_col] = quadro[hora_col]/100
    return quadro

def os_sheets(os_file):
    df = pd.read_excel(os_file, None)
    return len(df.keys())

def check_os_filetype(os_file):
    try:
        pd.read_excel(os_file)
        return True
    except ValueError:
        return False

def check_os_filename(os_file):
    pattern = re.compile(r'^os_\d{4}-\d{2}-\d{2}.xlsx$')
    return bool(pattern.match(os_file.name))


def check_os_columns(os_df):
    cols = sorted(list(os_df.columns))
    return bool(cols == sorted(constants.OS_COLUMNS.value))


def check_os_columns_order(os_df):
    cols = list(os_df.columns)
    return bool(cols == constants.OS_COLUMNS.value)


def check_gtfs_filename(gtfs_file):
    pattern = re.compile(r'^gtfs_\d{4}-\d{2}-\d{2}.zip$')
    return bool(pattern.match(gtfs_file.name))


def change_feed_info_dates(file: bytes, os_initial_date: datetime, os_final_date: datetime) -> bytes:

    # Descompacta o arquivo zip direto na memoria
    input_zip = ZipFile(io.BytesIO(file), 'r')
    files: dict[str, bytes] = {name: input_zip.read(name) for name in input_zip.namelist()}

    # Transforma os bytes em arquivo
    file_text = str(files['feed_info.txt'],'utf-8')    
    string_buffer = io.StringIO(file_text) 

    # Muda a data
    df = pd.read_csv(string_buffer)
    df['feed_start_date'] = os_initial_date.strftime('%Y%m%d')
    df['feed_end_date'] = os_final_date.strftime('%Y%m%d')

    # Transforma o dataframe em csv na memoria
    string_buffer = io.StringIO() 
    df.to_csv(string_buffer, index=False, lineterminator='\r\n')
    
    files['feed_info.txt'] = bytes(string_buffer.getvalue(), encoding='utf8')

    # Compacta o arquivo novamente
    zip_buffer = io.BytesIO()
    with ZipFile(zip_buffer, "w", ZIP_DEFLATED, False) as zip_file:
        for file_name, file_data in files.items():
            zip_file.writestr(file_name, io.BytesIO(file_data).getvalue())

    return zip_buffer
