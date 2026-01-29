# -*- coding: utf-8 -*-
"""
Flows de tratamento dos dados de monitoramento

DBT 2026-01-29
"""

from copy import deepcopy
from datetime import time

from pipelines.capture.cittati.constants import constants as cittati_constants
from pipelines.capture.conecta.constants import constants as conecta_constants
from pipelines.capture.jae.constants import constants as jae_constants
from pipelines.capture.rioonibus.constants import (
    constants as rioonibus_source_constants,
)
from pipelines.capture.veiculo_fiscalizacao.constants import (
    constants as veiculo_fiscalizacao_constants,
)
from pipelines.capture.zirix.constants import constants as zirix_constants
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import create_hourly_cron
from pipelines.treatment.cadastro.constants import constants as cadastro_constants
from pipelines.treatment.monitoramento.constants import constants
from pipelines.treatment.planejamento.constants import (
    constants as planejamento_constants,
)
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.utils.prefect import handler_notify_failure, set_default_parameters

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
        constants.GPS_CONECTA_SELECTOR.value,
        constants.GPS_CITTATI_SELECTOR.value,
        constants.GPS_ZIRIX_SELECTOR.value,
    ],
)

GPS_TEST_SCHEDULE_TIME = time(2, 6, 0)

GPS_CONECTA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps conecta - materializacao",
    selector=constants.GPS_CONECTA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        conecta_constants.CONECTA_REGISTROS_SOURCE.value,
        conecta_constants.CONECTA_REALOCACAO_SOURCE.value,
    ],
    test_scheduled_time=GPS_TEST_SCHEDULE_TIME,
    post_tests=constants.GPS_DAILY_TEST.value,
)
gps_vars_conecta = {"modo_gps": "onibus", "fonte_gps": "conecta", "15_minutos": False}
set_default_parameters(GPS_CONECTA_MATERIALIZACAO, {"additional_vars": gps_vars_conecta})

GPS_CITTATI_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps cittati - materializacao",
    selector=constants.GPS_CITTATI_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cittati_constants.CITTATI_REGISTROS_SOURCE.value,
        cittati_constants.CITTATI_REALOCACAO_SOURCE.value,
    ],
    test_scheduled_time=GPS_TEST_SCHEDULE_TIME,
    post_tests=constants.GPS_DAILY_TEST.value,
)
gps_vars_cittati = {"modo_gps": "onibus", "fonte_gps": "cittati", "15_minutos": False}
set_default_parameters(GPS_CITTATI_MATERIALIZACAO, {"additional_vars": gps_vars_cittati})

GPS_ZIRIX_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps zirix - materializacao",
    selector=constants.GPS_ZIRIX_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        zirix_constants.ZIRIX_REGISTROS_SOURCE.value,
        zirix_constants.ZIRIX_REALOCACAO_SOURCE.value,
    ],
    test_scheduled_time=GPS_TEST_SCHEDULE_TIME,
    post_tests=constants.GPS_DAILY_TEST.value,
)
gps_vars_zirix = {"modo_gps": "onibus", "fonte_gps": "zirix", "15_minutos": False}
set_default_parameters(GPS_ZIRIX_MATERIALIZACAO, {"additional_vars": gps_vars_zirix})

GPS_15_MINUTOS_CONECTA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps_15_minutos conecta - materializacao",
    selector=constants.GPS_15_MINUTOS_CONECTA_SELECTOR.value,
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
    selector=constants.GPS_15_MINUTOS_CITTATI_SELECTOR.value,
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
    selector=constants.GPS_15_MINUTOS_ZIRIX_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        zirix_constants.ZIRIX_REGISTROS_SOURCE.value,
        zirix_constants.ZIRIX_REALOCACAO_SOURCE.value,
    ],
)
gps_15_vars_zirix = {"modo_gps": "onibus", "fonte_gps": "zirix", "15_minutos": True}
set_default_parameters(GPS_15_MINUTOS_ZIRIX_MATERIALIZACAO, {"additional_vars": gps_15_vars_zirix})

MONITORAMENTO_VEICULO_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="monitoramento_veiculo - materializacao",
    selector=constants.MONITORAMENTO_VEICULO_SELECTOR.value,
    snapshot_selector=constants.SNAPSHOT_VEICULO_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[veiculo_fiscalizacao_constants.VEICULO_LACRE_SOURCE.value],
    post_tests=constants.MONITORAMENTO_VEICULO_TEST.value,
)

wait_monitoramento_veiculo = deepcopy(constants.MONITORAMENTO_VEICULO_SELECTOR.value)
wait_monitoramento_veiculo.incremental_delay_hours = (
    -constants.VEICULO_DIA_SELECTOR.value.incremental_delay_hours
)

wait_cadastro_veiculo = deepcopy(cadastro_constants.CADASTRO_VEICULO_SELECTOR.value)
wait_cadastro_veiculo.incremental_delay_hours = (
    -constants.VEICULO_DIA_SELECTOR.value.incremental_delay_hours
)

VEICULO_DIA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="veiculo_dia - materializacao",
    selector=constants.VEICULO_DIA_SELECTOR.value,
    snapshot_selector=constants.SNAPSHOT_VEICULO_DIA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        wait_monitoramento_veiculo,
        wait_cadastro_veiculo,
    ],
    post_tests=constants.VEICULO_DIA_TEST.value,
)

MONITORAMENTO_TEMPERATURA_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="monitoramento_temperatura - materializacao",
    selector=constants.MONITORAMENTO_TEMPERATURA_SELECTOR.value,
    snapshot_selector=constants.SNAPSHOT_TEMPERATURA_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[constants.MONITORAMENTO_VEICULO_SELECTOR.value],
    post_tests=constants.MONITORAMENTO_TEMPERATURA_TEST.value,
)

GPS_VALIDADOR_MATERIALIZACAO = create_default_materialization_flow(
    flow_name="gps_validador - materializacao",
    selector=constants.GPS_VALIDADOR_SELECTOR.value,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    wait=[
        cadastro_constants.CADASTRO_SELECTOR.value,
        jae_constants.GPS_VALIDADOR_SOURCE.value,
    ],
    post_tests=constants.GPS_VALIDADOR_DAILY_TEST.value,
    test_webhook_key=jae_constants.ALERT_WEBHOOK.value,
    test_scheduled_time=time(1, 15, 0),
    skip_if_running_tolerance=10,
)

GPS_VALIDADOR_MATERIALIZACAO.state_handlers.append(
    handler_notify_failure(webhook="alertas_bilhetagem")
)
