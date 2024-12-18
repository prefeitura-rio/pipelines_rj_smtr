# -*- coding: utf-8 -*-
"""Flows de captura dos dados da Jaé"""
from prefect import case
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.capture.jae.constants import constants
from pipelines.capture.jae.tasks import (
    create_database_error_discord_message,
    create_jae_general_extractor,
    test_jae_databases_connections,
)
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.tasks import log_discord
from pipelines.utils.prefect import set_default_parameters

CAPTURA_TRANSACAO_ORDEM = create_default_capture_flow(
    flow_name="jae: transacao_ordem - captura",
    source=constants.TRANSACAO_ORDEM_SOURCE.value,
    create_extractor_task=create_jae_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
)
set_default_parameters(CAPTURA_TRANSACAO_ORDEM, {"recapture": True})

with Flow("jae: verifica ip do banco de dados") as verificacao_ip:
    success, failed_connections = test_jae_databases_connections()
    with case(success, False):
        message = create_database_error_discord_message(failed_connections=failed_connections)
        send_discord_message = log_discord(
            message=message,
            key=constants.ALERT_WEBHOOK.value,
            dados_tag=True,
        )
    verificacao_ip.set_reference_tasks(tasks=[send_discord_message, success])
