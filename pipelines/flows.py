# -*- coding: utf-8 -*-
"""
Imports all flows for every project so we can register all of them.
"""
from pipelines.br_rj_riodejaneiro_brt_gps.flows import *  # noqa
from pipelines.capture.jae.flows import *  # noqa
from pipelines.capture.templates.flows import *  # noqa
from pipelines.exemplo import *  # noqa
from pipelines.gps_onibus_teste.flows import *  # noqa
from pipelines.treatment.bilhetagem.flows import *  # noqa
