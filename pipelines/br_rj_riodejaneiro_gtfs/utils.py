import io
import pandas as pd
import requests
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.constants import constants


def filter_valid_rows(df: pd.DataFrame) -> pd.DataFrame:
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
    return df


def download_controle_os_csv(url):

    response = requests.get(url=url, timeout=constants.MAX_TIMEOUT_SECONDS.value)
    response.raise_for_status()  # Verifica se houve algum erro na requisição
    response.encoding = "utf-8"
    # Carrega o conteúdo da resposta em um DataFrame
    df = pd.read_csv(io.StringIO(response.text))

    log(f"Download concluído! Dados:\n{df.head()}")
    return df


def convert_to_float(value):
    if "," in value:
        value = value.replace(".", "").replace(",", ".").strip()
    return float(value)
