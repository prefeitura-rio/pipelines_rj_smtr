# -*- coding: utf-8 -*-
"""Flows de captura dos dados da ZIRIX"""
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.capture.zirix.constants import constants
from pipelines.capture.zirix.tasks import create_gps_extractor
from pipelines.constants import constants as smtr_constants
from pipelines.schedules import create_hourly_cron

CAPTURA_REGISTROS_ZIRIX = create_default_capture_flow(
    flow_name="zirix: registros - captura",
    source=constants.ZIRIX_REGISTROS_SOURCE.value,
    create_extractor_task=create_gps_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
)

CAPTURA_REALOCACAO_ZIRIX = create_default_capture_flow(
    flow_name="zirix: realocacao - captura",
    source=constants.ZIRIX_REALOCACAO_SOURCE.value,
    create_extractor_task=create_gps_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    recapture_schedule_cron=create_hourly_cron(),
)
