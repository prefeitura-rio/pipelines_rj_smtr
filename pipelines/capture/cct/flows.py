# -*- coding: utf-8 -*-
# """Flows de captura dos dados da CCT"""

from pipelines.capture.cct.constants import constants
from pipelines.capture.cct.tasks import create_cct_general_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants
from pipelines.utils.prefect import set_default_parameters

CAPTURA_PAGAMENTO = create_default_capture_flow(
    flow_name="cct: pagamento - captura",
    source=constants.PAGAMENTO_SOURCES.value,
    create_extractor_task=create_cct_general_extractor,
    agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
    get_raw_max_retries=0,
)
set_default_parameters(CAPTURA_PAGAMENTO, {"recapture": True})
