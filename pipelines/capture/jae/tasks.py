# -*- coding: utf-8 -*-
"""Tasks for pipelines.capture.jae"""
from datetime import datetime
from typing import Union

from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.utils.capture.db import DBExtractor, PaginatedDBExtractor
from pipelines.utils.incremental_capture_strategy import IncrementalInfo
from pipelines.utils.jinja import render_template
from pipelines.utils.prefect import extractor_task
from pipelines.utils.secret import get_secret
from pipelines.utils.utils import create_sql_update_filter


@extractor_task
def create_extractor_jae(
    env: str,
    dataset_id: str,
    table_id: str,
    save_filepath: str,
    data_extractor_params: dict,
    incremental_info: IncrementalInfo,
) -> Union[DBExtractor, PaginatedDBExtractor]:
    """Cria o extrator de dados para capturas da JAE"""
    credentials = get_secret("smtr_jae_access_data")
    database = data_extractor_params["database"]
    database_details = jae_constants.JAE_DATABASES.value[database]

    start = incremental_info.start_value
    end = incremental_info.end_value

    if isinstance(start, datetime):
        start = start.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(end, datetime):
        end = end.strftime("%Y-%m-%d %H:%M:%S")

    template_variables = {
        "start": start,
        "end": end,
    }

    if "get_updates" in data_extractor_params.keys():
        template_variables["update"] = create_sql_update_filter(
            env=env,
            dataset_id=dataset_id,
            table_id=table_id,
            columns_to_search=data_extractor_params["get_updates"],
        )

    query = render_template(
        template_string=data_extractor_params["query"],
        execution_mode=incremental_info.execution_mode,
        _vars=template_variables,
    )

    extractor_general_args = {
        "query": query,
        "engine": database_details["engine"],
        "host": database_details["host"],
        "user": credentials["user"],
        "password": credentials["password"],
        "database": database,
        "save_filepath": save_filepath,
    }

    if table_id == jae_constants.GPS_VALIDADOR_CAPTURE_PARAMS.value["table_id"]:
        return PaginatedDBExtractor(
            page_size=data_extractor_params["page_size"],
            max_pages=data_extractor_params["max_pages"],
            **extractor_general_args,
        )

    return DBExtractor(**extractor_general_args)
