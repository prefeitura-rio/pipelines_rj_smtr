# -*- coding: utf-8 -*-
"""Module to get data from Google Drive"""
import os
from typing import List, Optional

import pandas as pd
from google.auth import default
from google.oauth2 import service_account
from googleapiclient.discovery import build

from pipelines.utils.prefect import flow_is_running_local
from pipelines.utils.pretreatment import normalize_text


def get_google_drive_service(service_name: str, version: str, scopes: Optional[List[str]] = None):
    """
    Retorna um serviço do Google Sheets configurado com as credenciais apropriadas.

    Args:
        service_name: Nome do serviço Google (ex: 'sheets', 'drive', 'bigquery')
        version: Versão da API (ex: 'v4', 'v3')
        scopes: Lista de escopos de permissão. Se None, usa drive.readonly por padrão.

    Returns:
        Resource: Serviço do Google Sheets API
    """

    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    if flow_is_running_local():
        creds, _ = default(scopes=scopes)
    else:
        creds = service_account.Credentials.from_service_account_file(
            filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes
        )

    return build(service_name, version, credentials=creds)


def get_google_sheet_xlsx(
    spread_sheet_id: str,
    sheet_name: str,
    filter_expr: Optional[str] = None,
) -> pd.DataFrame:

    drive_service = get_google_drive_service(service_name="sheets", version="v4")

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
