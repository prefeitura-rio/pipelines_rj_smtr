# -*- coding: utf-8 -*-
"""Flows de tratamento da bilhetagem"""
# from datetime import timedelta

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_inject_bd_credentials,
    handler_skip_if_running,
)

from pipelines.capture.jae.constants import constants as jae_capture_constants
from pipelines.capture.jae.flows import JAE_AUXILIAR_CAPTURE
from pipelines.constants import constants

# from pipelines.schedules import generate_interval_schedule
from pipelines.tasks import (  # parse_timestamp_to_string,
    get_scheduled_timestamp,
    run_subflow,
)
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.treatment.templates.tasks import create_date_range_variable

# from pipelines.utils.dataplex import DataQualityCheckArgs

BILHETAGEM_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="Bilhetagem - Materialização (subflow)",
    dataset_id="bilhetagem",
    datetime_column_name="datetime_processamento",
    create_datetime_variables_task=create_date_range_variable,
    overwrite_flow_param_values={
        "table_id": "transacao",
        "upstream": True,
    },
    agent_label=constants.RJ_SMTR_DEV_AGENT_LABEL.value,
    # data_quality_checks=[
    #     DataQualityCheckArgs(check_id="teste-falha", table_partition_column_name="data")
    # ],
)

with Flow("Bilhetagem - Tratamento") as bilhetagem_tratamento:

    timestamp = get_scheduled_timestamp()

    AUXILIAR_CAPTURE = run_subflow(
        flow_name=JAE_AUXILIAR_CAPTURE.name,
        parameters=jae_capture_constants.AUXILIAR_TABLE_CAPTURE_PARAMS.value,
        maximum_parallelism=3,
    )

    AUXILIAR_CAPTURE.name = "run_captura_auxiliar_jae"

    # TRANSACAO_MATERIALIZACAO = run_subflow(
    #     flow_name=BILHETAGEM_MATERIALIZACAO.name,
    #     parameters={"timestamp": parse_timestamp_to_string(timestamp=timestamp, pattern="iso")},
    #     upstream_tasks=[AUXILIAR_CAPTURE],
    # )
    # TRANSACAO_MATERIALIZACAO.name = "run_materializacao_transacao"


bilhetagem_tratamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bilhetagem_tratamento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMTR_AGENT_LABEL.value],
)

bilhetagem_tratamento.state_handlers = [
    handler_inject_bd_credentials,
    handler_skip_if_running,
]

# bilhetagem_tratamento.schedule = generate_interval_schedule(
#     interval=timedelta(hours=1),
#     agent_label=constants.RJ_SMTR_AGENT_LABEL.value,
# )
