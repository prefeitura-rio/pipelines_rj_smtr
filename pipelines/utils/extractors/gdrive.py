# -*- coding: utf-8 -*-
"""Module to get data from Google Drive"""
import os
from typing import Optional
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pipelines.utils.pretreatment import normalize_text

# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow


def get_google_sheet_xlsx(
    spread_sheet_id: str,
    sheet_name: str,
    filter_expr: Optional[str] = None,
) -> pd.DataFrame:
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_file(
        filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=SCOPES,
    )
    # creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    # if os.path.exists("token.json"):
    #     creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # # If there are no (valid) credentials available, let the user log in.
    # if not creds or not creds.valid:
    #     if creds and creds.expired and creds.refresh_token:
    #         creds.refresh(Request())
    #     else:
    #         flow = InstalledAppFlow.from_client_secrets_file("cliente_oauth_gdrive.json", SCOPES)
    #         creds = flow.run_local_server(port=0)
    #         # Save the credentials for the next run
    #     with open("token.json", "w") as token:
    #         token.write(creds.to_json())
    drive_service = build("sheets", "v4", credentials=creds)

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
