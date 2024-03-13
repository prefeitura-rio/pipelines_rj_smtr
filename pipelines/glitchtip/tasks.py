# -*- coding: utf-8 -*-
from datetime import datetime
import requests

from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.constants import constants
from pipelines.utils.secret import get_secret
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
    
@task
def glitch_api_get(query='is:unresolved'):
    base_url = constants.GLITCH_API_ENDPOINT.value

    headers = get_secret(secret_path=constants.GLITCH_AUTH.value)
    print(headers)
    if query:
        base_url += f'?query={query}'
    print(base_url)
    issues = requests.get(base_url, headers=headers)

    return issues.json()

@task
def format_glitch_issue_messages(issues):
    messages = []
    issue_count = len(issues)
    base_message = f"**Issues não resolvidos: {issue_count}**"
    messages.append(base_message)
    for issue in issues:
        msg = f"""**Fonte: {issue['culprit']}**
**Erro**: {issue['title']}
**Event Count:** {issue['count']}
**Criado:** {issue['firstSeen'].split('.')[0]}
**Link:** {constants.GLITCH_URL.value}/smtr/issues/{issue['id']}
"""
        messages.append({'name':f"Issue {issue['id']}", 'value':msg})
    
    return messages

@task
def send_issue_report(messages):
    timestamp = datetime.now().isoformat()
    webhook = get_secret(
        secret_path=constants.WEBHOOKS_SECRET_PATH.value, 
        secret_name=constants.GLITCH_WEBHOOK.value
    )['glitch']
    headers = {"Content-Type": "application/json"}
    message = {
            "content": messages[0],
            "embeds": [{
                "color": 16515072,
                "timestamp": timestamp,
                "author": {'name':"Glitch Tip Issues", 'url':constants.GLITCH_URL.value},
                "fields": messages[1:]
            }]
            }
    response = requests.post(url=webhook, headers=headers, json=message)

    log(response.text)
    log(response.status_code)

