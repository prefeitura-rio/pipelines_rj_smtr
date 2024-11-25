# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de bilhetagem

DBT: 2024-11-25
"""

from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_bilhetagem.constants import (
    constants as old_constants,
)
from pipelines.schedules import create_daily_cron
from pipelines.treatment.bilhetagem.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow

ordem_pagamento_materialize_params = (
    old_constants.BILHETAGEM_MATERIALIZACAO_ORDEM_PAGAMENTO_PARAMS.value
)

TRANSACAO_ORDEM_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="transacao_ordem - materializacao",
    selector=constants.TRANSACAO_ORDEM_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        {
            "redis_key": f"{ordem_pagamento_materialize_params['dataset_id']}\
.{ordem_pagamento_materialize_params['table_id']}",
            "dict_key": "last_run_timestamp",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "delay_hours": ordem_pagamento_materialize_params["dbt_vars"]["date_range"][
                "delay_hours"
            ],
            "schedule_cron": create_daily_cron(hour=5),
        }
    ],
)
