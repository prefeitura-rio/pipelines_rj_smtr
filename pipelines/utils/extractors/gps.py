# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from pipelines.utils.extractors.api import get_raw_api
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


def create_generic_gps_extractor(
    source: SourceTable,
    base_url: str,
    registros_endpoint: str,
    realocacao_endpoint: str,
    secret_path: str,
    registros_table_id: str,
    timestamp: datetime,
    registros_start_minutes: int = 6,
    registros_end_minutes: int = 5,
    realocacao_minutes: int = 10,
):
    """Base function to create GPS data extractors for different APIs"""

    if source.table_id == registros_table_id:
        url = f"{base_url}/{registros_endpoint}?"
        date_range = {
            "date_range_start": (timestamp - timedelta(minutes=registros_start_minutes)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "date_range_end": (timestamp - timedelta(minutes=registros_end_minutes)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
    else:
        url = f"{base_url}/{realocacao_endpoint}?"
        date_range = {
            "date_range_start": (timestamp - timedelta(minutes=realocacao_minutes)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "date_range_end": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        }

    headers = get_secret(secret_path)
    key = list(headers)[0]

    params = {
        "guidIdentificacao": headers[key],
        "dataInicial": date_range["date_range_start"],
        "dataFinal": date_range["date_range_end"],
    }

    return get_raw_api(url=url, params=params)
