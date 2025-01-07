# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados da validação dos dados da Jaé
"""

from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_bilhetagem.constants import (
    constants as bilhetagem_constants,
)
from pipelines.schedules import create_daily_cron, create_hourly_cron
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.treatment.validacao_dados_jae.constants import constants

transacao_materializacao_params = (
    bilhetagem_constants.BILHETAGEM_MATERIALIZACAO_TRANSACAO_PARAMS.value
)

integracao_materializacao_params = (
    bilhetagem_constants.BILHETAGEM_MATERIALIZACAO_INTEGRACAO_PARAMS.value
)

VALIDACAO_DADOS_JAE_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="validacao_dados_jae - materializacao",
    selector=constants.VALIDACAO_DADOS_JAE_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        {
            "redis_key": f"{integracao_materializacao_params['dataset_id']}\
.{integracao_materializacao_params['table_id']}",
            "dict_key": "last_run_timestamp",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "delay_hours": integracao_materializacao_params["dbt_vars"]["date_range"][
                "delay_hours"
            ],
            "schedule_cron": create_daily_cron(hour=5),
        },
        {
            "redis_key": f"{transacao_materializacao_params['dataset_id']}\
.{transacao_materializacao_params['table_id']}",
            "dict_key": "last_run_timestamp",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "delay_hours": transacao_materializacao_params["dbt_vars"]["date_range"]["delay_hours"],
            "schedule_cron": create_hourly_cron(),
        },
    ],
)
