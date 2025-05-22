# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de monitoramento

DBT 2025-02-03
"""

from copy import deepcopy

from pipelines.capture.cittati.constants import constants as cittati_constants
from pipelines.capture.conecta.constants import constants as conecta_constants
from pipelines.capture.rioonibus.constants import (
    constants as rioonibus_source_constants,
)
from pipelines.capture.zirix.constants import constants as zirix_constants
from pipelines.constants import constants as smtr_constants
from pipelines.migration.br_rj_riodejaneiro_onibus_gps_zirix.constants import (
    constants as gps_zirix_constants,
)
from pipelines.schedules import create_hourly_cron
from pipelines.treatment.monitoramento.constants import constants
from pipelines.treatment.planejamento.constants import (
    constants as planejamento_constants,
)
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.utils.prefect import set_default_parameters

cron_every_hour_minute_6 = create_hourly_cron(minute=6)

VIAGEM_INFORMADA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="viagem_informada - materializacao",
    selector=constants.VIAGEM_INFORMADA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR.value,
        rioonibus_source_constants.VIAGEM_INFORMADA_SOURCE.value,
    ],
)

wait_viagem_informada = deepcopy(constants.VIAGEM_INFORMADA_SELECTOR.value)
wait_viagem_informada.incremental_delay_hours = (
    -constants.VIAGEM_VALIDACAO_SELECTOR.value.incremental_delay_hours
)

VIAGEM_VALIDACAO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="viagem_validacao - materializacao",
    selector=constants.VIAGEM_VALIDACAO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        wait_viagem_informada,
        planejamento_constants.PLANEJAMENTO_DIARIO_SELECTOR.value,
        {
            "redis_key": f"{smtr_constants.GPS_SPPO_DATASET_ID.value}\
.{smtr_constants.GPS_SPPO_TREATED_TABLE_ID.value}",
            "dict_key": "last_run_timestamp",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "delay_hours": smtr_constants.GPS_SPPO_MATERIALIZE_DELAY_HOURS.value,
            "schedule_cron": cron_every_hour_minute_6,
        },
        {
            "redis_key": f"{gps_zirix_constants.GPS_SPPO_ZIRIX_RAW_DATASET_ID.value}\
.{smtr_constants.GPS_SPPO_TREATED_TABLE_ID.value}",
            "dict_key": "last_run_timestamp",
            "datetime_format": "%Y-%m-%dT%H:%M:%S",
            "delay_hours": smtr_constants.GPS_SPPO_MATERIALIZE_DELAY_HOURS.value,
            "schedule_cron": cron_every_hour_minute_6,
        },
    ],
)

GPS_CONECTA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps conecta - materializacao",
    selector=constants.GPS_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        conecta_constants.CONECTA_REGISTROS_SOURCE.value,
        conecta_constants.CONECTA_REALOCACAO_SOURCE.value,
    ],
)
gps_vars_conecta = {"modo_gps": "onibus", "fonte_gps": "conecta", "15_minutos": False}
set_default_parameters(GPS_CONECTA_MATERIALIZACAO, {"additional_vars": gps_vars_conecta})

GPS_CITTATI_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps cittati - materializacao",
    selector=constants.GPS_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cittati_constants.CITTATI_REGISTROS_SOURCE.value,
        cittati_constants.CITTATI_REALOCACAO_SOURCE.value,
    ],
)
gps_vars_cittati = {"modo_gps": "onibus", "fonte_gps": "cittati", "15_minutos": False}
set_default_parameters(GPS_CITTATI_MATERIALIZACAO, {"additional_vars": gps_vars_cittati})

GPS_ZIRIX_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps zirix - materializacao",
    selector=constants.GPS_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        zirix_constants.ZIRIX_REGISTROS_SOURCE.value,
        zirix_constants.ZIRIX_REALOCACAO_SOURCE.value,
    ],
)
gps_vars_zirix = {"modo_gps": "onibus", "fonte_gps": "zirix", "15_minutos": False}
set_default_parameters(GPS_ZIRIX_MATERIALIZACAO, {"additional_vars": gps_vars_zirix})

GPS_15_MINUTOS_CONECTA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps_15_minutos conecta - materializacao",
    selector=constants.GPS_15_MINUTOS_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        conecta_constants.CONECTA_REGISTROS_SOURCE.value,
        conecta_constants.CONECTA_REALOCACAO_SOURCE.value,
    ],
)
gps_15_vars_conecta = {"modo_gps": "onibus", "fonte_gps": "conecta", "15_minutos": True}
set_default_parameters(
    GPS_15_MINUTOS_CONECTA_MATERIALIZACAO, {"additional_vars": gps_15_vars_conecta}
)

GPS_15_MINUTOS_CITTATI_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps_15_minutos cittati - materializacao",
    selector=constants.GPS_15_MINUTOS_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cittati_constants.CITTATI_REGISTROS_SOURCE.value,
        cittati_constants.CITTATI_REALOCACAO_SOURCE.value,
    ],
)
gps_15_vars_cittati = {"modo_gps": "onibus", "fonte_gps": "cittati", "15_minutos": True}
set_default_parameters(
    GPS_15_MINUTOS_CITTATI_MATERIALIZACAO, {"additional_vars": gps_15_vars_cittati}
)

GPS_15_MINUTOS_ZIRIX_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps_15_minutos zirix - materializacao",
    selector=constants.GPS_15_MINUTOS_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        zirix_constants.ZIRIX_REGISTROS_SOURCE.value,
        zirix_constants.ZIRIX_REALOCACAO_SOURCE.value,
    ],
)
gps_15_vars_zirix = {"modo_gps": "onibus", "fonte_gps": "zirix", "15_minutos": True}
set_default_parameters(GPS_15_MINUTOS_ZIRIX_MATERIALIZACAO, {"additional_vars": gps_15_vars_zirix})
