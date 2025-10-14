# -*- coding: utf-8 -*-
"""Flows para exportação das transações do BQ para o Postgres"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as smtr_constants
from pipelines.upload_transacao_cct.tasks import upload_files_postgres

with Flow(name="cct: transacao_cct postgresql - upload") as upload_transacao_cct:
    upload_files_postgres()

upload_transacao_cct.storage = GCS(smtr_constants.GCS_FLOWS_BUCKET.value)
upload_transacao_cct.run_config = KubernetesRun(
    image=smtr_constants.DOCKER_IMAGE.value,
    labels=[smtr_constants.RJ_SMTR_AGENT_LABEL.value],
)
upload_transacao_cct.state_handlers = [
    handler_inject_bd_credentials,
    handler_initialize_sentry,
]
