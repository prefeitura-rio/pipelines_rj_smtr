# -*- coding: utf-8 -*-
"""Module to get data from Google Drive"""
import os
from typing import Optional

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from pipelines.utils.pretreatment import normalize_text


def get_google_sheet_xlsx(
    spread_sheet_id: str,
    sheet_name: str,
    filter_expr: Optional[str] = None,
) -> pd.DataFrame:
    credentials = service_account.Credentials.from_service_account_file(
        filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    drive_service = build("sheets", "v4", credentials=credentials)

    file = (
        drive_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spread_sheet_id,
            range=sheet_name,
        )
        .execute()
    )["values"]

    df = pd.DataFrame(file[1:], columns=file[0])

    df.columns = [normalize_text(c) for c in df.columns]
    if filter_expr:
        df = df.query(filter_expr)

    return df
