# -*- coding: utf-8 -*-
"""Tasks para exportação das transações do BQ para o Postgres"""
import psycopg2
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.capture.cct.constants import CCT_PRIVATE_BUCKET_NAMES
from pipelines.capture.cct.constants import constants as cct_constants
from pipelines.upload_transacao_cct.constants import constants
from pipelines.utils.gcp.storage import Storage
from pipelines.utils.secret import get_secret


def copy_gcs_to_postgres(blob_name, table_name):
    storage = Storage(env="prod", dataset_id="transacao_cct", bucket_names=CCT_PRIVATE_BUCKET_NAMES)
    bucket = storage.bucket
    blob = bucket.blob(blob_name)
    credentials = get_secret(cct_constants.CCT_SECRET_PATH.value)
    with psycopg2.connect(
        host=credentials["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["dbname"],
    ) as conn:
        with conn.cursor() as cur:
            sql = f"""
                COPY {table_name}
                FROM STDIN WITH CSV HEADER
            """
            with blob.open("r") as f:  # stream direto do GCS
                cur.copy_expert(sql, f)
        conn.commit()


@task
def upload_files_postgres():
    storage = Storage(env="prod", dataset_id="transacao_cct", bucket_names=CCT_PRIVATE_BUCKET_NAMES)
    bucket = storage.bucket
    blobs = bucket.list_blobs(prefix="upload/transacao_cct/")

    tmp_table_name = f"tmp__{constants.TRANSACAO_POSTGRES_TABLE_NAME.value}"

    credentials = get_secret(cct_constants.CCT_SECRET_PATH.value)

    with psycopg2.connect(
        host=credentials["host"],
        user=credentials["user"],
        password=credentials["password"],
        database=credentials["dbname"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{tmp_table_name}")
            cur.execute(f"TRUNCATE TABLE public.{constants.TRANSACAO_POSTGRES_TABLE_NAME.value}")
            sql = f"""
                CREATE TABLE IF NOT EXISTS public.{tmp_table_name}
                (
                    id_transacao character varying(60),
                    data date,
                    datetime_transacao timestamp without time zone,
                    consorcio character varying(20),
                    tipo_transacao character varying(50),
                    valor_pagamento numeric(13,5),
                    id_ordem_pagamento integer,
                    id_ordem_pagamento_consorcio_operador_dia integer,
                    datetime_ultima_atualizacao timestamp without time zone
                )
            """
            log("Criando tabela temporária")
            cur.execute(sql)
            conn.commit()
            log("Tabela temporária criada")

            for blob in blobs:
                log(f"Copiando arquivo {blob.name} para o Postgres")
                sql = f"""
                    COPY {constants.TRANSACAO_POSTGRES_TABLE_NAME.value}
                    FROM STDIN WITH CSV HEADER
                """
                with blob.open("r") as f:
                    cur.copy_expert(sql, f)
                conn.commit()
                log("Cópia completa")
