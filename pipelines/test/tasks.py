# -*- coding: utf-8 -*-
from datetime import datetime

from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.backup.utils import log_critical, map_dict_keys

# EMD Imports #


# SMTR Imports #



@task
def test_raise_errors(datetime: datetime):
    if datetime.minute % 5 == 0:
        raise ValueError(f"{datetime} % 5 is equal to zero")
    if datetime.minute % 3 == 0:
        raise ValueError(f"{datetime} % 3 is equal to zero")
    else:
        return datetime.minute / 0
