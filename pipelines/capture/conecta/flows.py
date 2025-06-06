# -*- coding: utf-8 -*-
"""Flows de captura dos dados da CONECTA"""
from pipelines.capture.conecta.constants import constants
from pipelines.capture.conecta.tasks import create_gps_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import create_hourly_cron

CAPTURA_REGISTROS_CONECTA = create_default_capture_flow(
    flow_name="conecta: registros - captura",
    source=constants.CONECTA_REGISTROS_SOURCE.value,
    create_extractor_task=create_gps_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
)

CAPTURA_REALOCACAO_CONECTA = create_default_capture_flow(
    flow_name="conecta: realocacao - captura",
    source=constants.CONECTA_REALOCACAO_SOURCE.value,
    create_extractor_task=create_gps_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
)
