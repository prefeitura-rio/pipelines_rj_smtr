# -*- coding: utf-8 -*-
"""Tasks for pipelines.capture.jae"""
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.capture.jae.constants import constants
from pipelines.utils.capture.db import DBExtractor
from pipelines.utils.incremental_strategy import IncrementalInfo
from pipelines.utils.jinja import render_template
from pipelines.utils.prefect import extractor_task
from pipelines.utils.secret import get_secret


@extractor_task
def create_extractor_jae(
    save_filepath: str,
    extract_params: dict,
    incremental_info: IncrementalInfo,
) -> DBExtractor:
    """Creates the Database Extractor for Jae capture flows"""

    log("oi")

    credentials = get_secret("smtr_jae_access_data")
    database = extract_params["database"]
    database_details = constants.JAE_DATABASES.value[database]
    engine = database_details["engine"]
    host = database_details["host"]

    query = render_template(
        template_string=extract_params["query"],
        execution_mode=incremental_info.execution_mode,
        _vars={"start": incremental_info.start_value, "end": incremental_info.end_value},
    )

    return DBExtractor(
        query=query,
        engine=engine,
        host=host,
        user=credentials["user"],
        password=credentials["password"],
        database=database,
        save_path=save_filepath,
    )
