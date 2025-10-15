# -*- coding: utf-8 -*-
"""Tasks para exportação das transações do BQ para o Postgres"""

from datetime import datetime

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from pytz import timezone
from sqlalchemy import create_engine

from pipelines.capture.cct.constants import constants as cct_constants
from pipelines.upload_transacao_cct.constants import constants
from pipelines.utils.database import create_database_url
from pipelines.utils.secret import get_secret


@task
def upload_files_postgres(quantidade_arquivos: int, timestamp=str):
    timestamp = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d-%H-%M")
    base_file_name = f"gs://rj-smtr-cct-private/upload/transacao_cct/{timestamp}"
    tmp_table_name = f"tmp__{constants.TRANSACAO_POSTGRES_TABLE_NAME.value}"

    credentials = get_secret(cct_constants.CCT_SECRET_PATH.value)

    url = create_database_url(
        engine="postgresql",
        host=credentials["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["dbname"],
    )
    connection = create_engine(url)

    df_list = []
    df_length = 0
    for i in range(quantidade_arquivos):

        file_name = f"{base_file_name}-{str(i).rjust(12, '0')}.csv"
        log(f"Lendo arquivo: {file_name}")
        df = pd.read_csv(file_name)

        df_list.append(df)
        df_length += len(df)

        log(f"df_length = {df_length}")

        if df_length >= 200_000:
            log("Escrevendo dados no postgres...")
            df_concat = pd.concat(df_list)

            df_concat["data"] = pd.to_datetime(df_concat.data)
            df_concat["datetime_transacao"] = pd.to_datetime(
                df_concat.datetime_transacao, format="mixed"
            ).dt.tz_localize(timezone("America/Sao_Paulo"))
            df_concat["datetime_ultima_atualizacao"] = pd.to_datetime(
                df_concat["datetime_ultima_atualizacao"]
            ).dt.tz_localize(timezone("America/Sao_Paulo"))
            df_concat["id_ordem_pagamento"] = df_concat["id_ordem_pagamento"].astype(int)
            df_concat["id_ordem_pagamento_consorcio_operador_dia"] = df_concat[
                "id_ordem_pagamento_consorcio_operador_dia"
            ].astype(int)

            df_list = []
            df_length = 0

            df_concat.to_sql(
                tmp_table_name,
                con=connection,
                if_exists="append",
                method="multi",
            )
            log("Dados adicionados")

    log("Escrevendo dados no postgres...")

    df_concat = pd.concat(df_list)

    df_concat["data"] = pd.to_datetime(df_concat.data)
    df_concat["datetime_transacao"] = pd.to_datetime(
        df_concat.datetime_transacao, format="mixed"
    ).dt.tz_localize(timezone("America/Sao_Paulo"))
    df_concat["datetime_ultima_atualizacao"] = pd.to_datetime(
        df_concat["datetime_ultima_atualizacao"]
    ).dt.tz_localize(timezone("America/Sao_Paulo"))
    df_concat["id_ordem_pagamento"] = df_concat["id_ordem_pagamento"].astype(int)
    df_concat["id_ordem_pagamento_consorcio_operador_dia"] = df_concat[
        "id_ordem_pagamento_consorcio_operador_dia"
    ].astype(int)

    df_concat.to_sql(
        tmp_table_name,
        con=connection,
        if_exists="append",
        method="multi",
    )

    log("Dados adicionados")
