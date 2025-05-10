# -*- coding: utf-8 -*-
"""Flows de captura dos dados da CITTATI"""
from pipelines.capture.cittati.constants import constants
from pipelines.capture.cittati.tasks import create_gps_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import create_hourly_cron

CAPTURA_REGISTROS_CITTATI = create_default_capture_flow(
    flow_name="cittati: registros - captura",
    source=constants.CITTATI_REGISTROS_SOURCE.value,
    create_extractor_task=create_gps_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    generate_schedule=False,
    recapture_schedule_cron=create_hourly_cron(),
)

CAPTURA_REALOCACAO_CITTATI = create_default_capture_flow(
    flow_name="cittati: realocacao - captura",
    source=constants.CITTATI_REALOCACAO_SOURCE.value,
    create_extractor_task=create_gps_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    generate_schedule=False,
    recapture_schedule_cron=create_hourly_cron(),
)
