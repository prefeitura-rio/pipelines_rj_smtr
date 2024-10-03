# -*- coding: utf-8 -*-
"""
General purpose functions for the br_rj_riodejaneiro_rdo project
"""

from datetime import datetime, timedelta

import pandas as pd
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pytz import timezone

from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_rdo.constants import constants


def build_table_id(mode: str, report_type: str):
    """Build table_id based on which table is the target
    of current flow run

    Args:
        mode (str): SPPO or STPL
        report_type (str): RHO or RDO

    Returns:
        str: table_id
    """
    if mode == "SPPO":
        if report_type == "RDO":
            table_id = constants.SPPO_RDO_TABLE_ID.value
        else:
            table_id = constants.SPPO_RHO_TABLE_ID.value
    if mode == "STPL":
        # slice the string to get rid of V at end of
        # STPL reports filenames
        if report_type[:3] == "RDO":
            table_id = constants.STPL_RDO_TABLE_ID.value
        else:
            table_id = constants.STPL_RHO_TABLE_ID.value
    return table_id


def merge_file_info_and_errors(files: list, errors: list):
    """

    Args:
        files (list): List of dicts
        errors (list): list of errors

    Returns:
        list: containing dicts with updated error
    """
    for i, file in enumerate(files):
        file["error"] = errors[i]
    return files


def generate_ftp_schedules(
    interval_minutes: int, label: str = smtr_constants.RJ_SMTR_AGENT_LABEL.value
):
    """Generates IntervalClocks with the parameters needed to capture
    each report.

    Args:
        interval_minutes (int): interval which this flow will be run.
        label (str, optional): Prefect label, defines which agent to use when launching flow run.
        Defaults to smtr_constants.RJ_SMTR_AGENT_LABEL.value.

    Returns:
        Schedule: Schedules for RDO/RHO data capture
    """
    modes = ["SPPO", "STPL"]
    reports = ["RDO", "RHO"]
    clocks = []
    for mode in modes:
        for report in reports:
            clocks.append(
                IntervalClock(
                    interval=timedelta(minutes=interval_minutes),
                    start_date=datetime(
                        2022, 12, 16, 5, 0, tzinfo=timezone(smtr_constants.TIMEZONE.value)
                    ),
                    parameter_defaults={
                        "transport_mode": mode,
                        "report_type": report,
                        "table_id": build_table_id(mode=mode, report_type=report),
                    },
                    labels=[label],
                )
            )
    return Schedule(clocks)


def read_raw_rdo(raw_filepath: str) -> pd.DataFrame:
    """
    Cria um DataFrame a partir arquivo csv usando o encoding utf-8 ou latin-1

    Args:
        raw_filepath (str): caminho do arquivo csv

    Returns:
        DataFrame: DataFrame com os dados do csv
    """
    try:
        return pd.read_csv(
            raw_filepath,
            header=None,
            delimiter=";",
            index_col=False,
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            raw_filepath,
            header=None,
            delimiter=";",
            index_col=False,
            encoding="latin-1",
        )
