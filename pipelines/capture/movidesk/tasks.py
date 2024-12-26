# -*- coding: utf-8 -*-
"""Tasks de captura dos tickets do MoviDesk"""
from datetime import datetime, timedelta
from functools import partial

from prefect import task

from pipelines.capture.movidesk.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.extractors.api import get_raw_api_top_skip
from pipelines.utils.gcp.bigquery import SourceTable
from pipelines.utils.secret import get_secret


@task(
    max_retries=smtr_constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=smtr_constants.RETRY_DELAY.value),
)
def create_tickets_extractor(
    source: SourceTable,  # pylint: disable=W0613
    timestamp: datetime,
):
    """Cria a extração dos tickets do MoviDesk"""

    start = datetime.strftime(timestamp - timedelta(days=1), "%Y-%m-%dT%H:%M:%S.%MZ")
    end = datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%MZ")
    token = get_secret(constants.MOVIDESK_SECRET_PATH.value)["token"]
    service = "Viagem Individual"
    params = {
        "token": token,
        "$select": "id,protocol,createdDate,lastUpdate",
        "$filter": f"serviceFirstLevel eq '{service} - Recurso Viagens Subsídio'and (lastUpdate ge {start} and lastUpdate lt {end} or createdDate ge {start} and createdDate lt {end})",  # noqa
        "$expand": "customFieldValues,customFieldValues($expand=items)",
        "$orderby": "createdDate asc",
    }

    return partial(
        get_raw_api_top_skip,
        url=constants.TICKETS_BASE_URL.value,
        headers=None,
        params=params,
        top_param_name="$top",
        skip_param_name="$skip",
        page_size=1000,
        max_page=10,
    )
