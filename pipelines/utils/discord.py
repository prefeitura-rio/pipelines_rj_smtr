# -*- coding: utf-8 -*-
from datetime import datetime

import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.secret import get_secret


def send_discord_embed_message(
    webhook_secret_key: str,
    content: str,
    author_name: str,
    author_url: str,
    embed_messages: list[dict[str]],
    timestamp: datetime,
):
    webhook = get_secret(
        secret_path=constants.WEBHOOKS_SECRET_PATH.value, secret_name=webhook_secret_key
    )[webhook_secret_key]
    headers = {"Content-Type": "application/json"}
    message = {
        "content": content,
        "embeds": [
            {
                "color": 16515072,
                "timestamp": timestamp.isoformat(),
                "author": {"name": author_name, "url": author_url},
                "fields": embed_messages,
            }
        ],
    }
    response = requests.post(
        url=webhook,
        headers=headers,
        json=message,
        timeout=constants.MAX_TIMEOUT_SECONDS.value,
    )

    log(response.text)
    log(response.status_code)
