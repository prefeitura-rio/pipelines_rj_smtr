# -*- coding: utf-8 -*-
"""Tasks para exportação das transações do BQ para o Postgres"""

import pandas as pd
from prefect import task
from pytz import timezone
from sqlalchemy import create_engine

from pipelines.capture.cct.constants import constants as cct_constants
from pipelines.upload_transacao_cct.constants import constants
from pipelines.utils.database import create_database_url
from pipelines.utils.secret import get_secret


@task
def upload_files_postgres():
    file_name = "gs://rj-smtr-cct-private/upload/transacao_cct/2025-10-14-15-42-000000000000.csv"
    df = pd.read_csv(file_name)
    df["data"] = pd.to_datetime(df.data)
    df["datetime_transacao"] = pd.to_datetime(df.datetime_transacao).dt.tz_localize(
        timezone("America/Sao_Paulo")
    )
    df["datetime_ultima_atualizacao"] = pd.to_datetime(
        df["datetime_ultima_atualizacao"]
    ).dt.tz_localize(timezone("America/Sao_Paulo"))
    df["id_ordem_pagamento"] = df["id_ordem_pagamento"].astype(int)
    df["id_ordem_pagamento_consorcio_operador_dia"] = df[
        "id_ordem_pagamento_consorcio_operador_dia"
    ].astype(int)

    credentials = get_secret(cct_constants.CCT_SECRET_PATH.value)

    url = create_database_url(
        engine="postgresql",
        host=credentials["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["dbname"],
    )
    connection = create_engine(url)
    tmp_table_name = f"tmp__{constants.TRANSACAO_POSTGRES_TABLE_NAME.value}"
    df.to_sql(
        tmp_table_name,
        con=connection,
        if_exists="append",
    )
