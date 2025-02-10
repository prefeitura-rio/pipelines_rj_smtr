# -*- coding: utf-8 -*-
import os
from datetime import datetime
from typing import Union

from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.redis_pal import get_redis_client

from pipelines.capture.jae.constants import constants
from pipelines.constants import constants as smtr_constants
from pipelines.utils.fs import get_data_folder_path
from pipelines.utils.utils import convert_timezone


def create_billingpay_backup_filepath(
    table_name: str,
    database_name: str,
    partition: str,
    timestamp: datetime,
) -> str:
    return os.path.join(
        get_data_folder_path(),
        constants.BACKUP_BILLING_PAY_FOLDER.value,
        database_name,
        table_name,
        partition,
        f"{timestamp.strftime(smtr_constants.FILENAME_PATTERN.value)}.json",
    )


def get_redis_last_backup(
    env: str, table_name: str, database_name: str, incremental_type: str
) -> Union[int, datetime]:
    redis_key = f"{env}.backup_jae_billingpay.{database_name}.{table_name}"
    log(f"Consultando Redis: {redis_key}")
    redis_client = get_redis_client(host="localhost")
    content = redis_client.get(redis_key)
    log(f"content = {content}")
    if incremental_type == "datetime":
        last_datetime = (
            datetime(1900, 1, 1, 0, 0, 0)
            if content is None
            else datetime.strptime(
                content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY.value],
                smtr_constants.MATERIALIZATION_LAST_RUN_PATTERN.value,
            )
        )

        return convert_timezone(timestamp=last_datetime)
    if incremental_type == "integer":
        last_id = (
            0
            if content is None
            else int(content[constants.BACKUP_BILLING_LAST_VALUE_REDIS_KEY.value])
        )
        return last_id

    raise ValueError(f"Tipo {incremental_type} não encontrado.")
