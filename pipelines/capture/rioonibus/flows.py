# -*- coding: utf-8 -*-
from pipelines.capture.rioonibus.constants import constants
from pipelines.capture.rioonibus.tasks import create_viagem_informada_extractor
from pipelines.capture.templates.flows import create_default_capture_flow
from pipelines.constants import constants as smtr_constants

captura_viagem_informada = create_default_capture_flow(
    flow_name="rioonibus: viagem_informada - captura",
    source=constants.VIAGEM_INFORMADA_SOURCE.value,
    create_extractor_task=create_viagem_informada_extractor,
    agent_label=smtr_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
)
