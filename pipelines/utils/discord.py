# -*- coding: utf-8 -*-
from datetime import datetime

import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.secret import get_secret


def send_discord_message(
    message: str,
    webhook_url: str,
) -> None:
    """
    Sends a message to a Discord channel.
    """
    requests.post(
        webhook_url,
        data={"content": message},
    )


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


def format_send_discord_message(formatted_messages: list, webhook_url: str):
    """
    Format and send a message to discord

    Args:
        formatted_messages (list): The formatted messages
        webhook_url (str): The webhook url

    Returns:
        None
    """
    formatted_message = "".join(formatted_messages)
    log(formatted_message)
    msg_ext = len(formatted_message)
    if msg_ext > 2000:
        log(f"** Message too long ({msg_ext} characters), will be split into multiple messages **")
        # Split message into lines
        lines = formatted_message.split("\n")
        message_chunks = []
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:  # +1 for the newline character
                message_chunks.append(chunk)
                chunk = ""
            chunk += line + "\n"
        message_chunks.append(chunk)  # Append the last chunk
        for chunk in message_chunks:
            send_discord_message(
                message=chunk,
                webhook_url=webhook_url,
            )
    else:
        send_discord_message(
            message=formatted_message,
            webhook_url=webhook_url,
        )
