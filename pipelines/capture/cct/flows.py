# -*- coding: utf-8 -*-
# """Flows de captura dos dados da CCT"""
# from datetime import datetime, timedelta

# from prefect import Parameter, case, unmapped
# from prefect.run_configs import KubernetesRun
# from prefect.schedules import Schedule
# from prefect.schedules.clocks import IntervalClock
# from prefect.storage import GCS
# from prefeitura_rio.pipelines_utils.custom import Flow
# from prefeitura_rio.pipelines_utils.state_handlers import (
#     handler_initialize_sentry,
#     handler_inject_bd_credentials,
# )
# from pytz import timezone

# from pipelines.capture.cct.constants import constants
# from pipelines.capture.cct.tasks import create_cct_general_extractor
# from pipelines.capture.templates.flows import create_default_capture_flow
# from pipelines.constants import constants as smtr_constants
# from pipelines.schedules import create_hourly_cron, every_day_hour_five, every_hour
# from pipelines.tasks import get_run_env, get_scheduled_timestamp, log_discord
# from pipelines.utils.prefect import set_default_parameters

# # Capturas minuto a minuto

# CAPTURA_TRANSACAO = create_default_capture_flow(
#     flow_name="jae: transacao - captura",
#     source=constants.TRANSACAO_SOURCE.value,
#     create_extractor_task=create_cct_general_extractor,
#     agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
#     recapture_schedule_cron=create_hourly_cron(),
#     get_raw_max_retries=0,
# )
