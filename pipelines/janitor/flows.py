# -*- coding: utf-8 -*-
"Flows for janitor"
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants as emd_constants
from pipelines.janitor.tasks import (
    cancel_flows,
    get_prefect_client,
    query_active_flow_names,
    query_not_active_flows,
)
from pipelines.schedules import every_5_minutes
from pipelines.utils.backup.tasks import get_flow_project

with Flow(
    "SMTR: Desagendamento de runs arquivadas",
) as janitor_flow:
    PROJECT_NAME = get_flow_project()
    client = get_prefect_client()
    flows = query_active_flow_names(prefect_client=client, prefect_project=PROJECT_NAME)
    archived_flow_runs = query_not_active_flows.map(
        flows=flows, prefect_client=unmapped(client), prefect_project=unmapped(PROJECT_NAME)
    )
    cancel_flows.map(flows=archived_flow_runs, prefect_client=unmapped(client))

janitor_flow.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
janitor_flow.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
janitor_flow.state_handlers = [handler_inject_bd_credentials, handler_initialize_sentry]
janitor_flow.schedule = every_5_minutes
