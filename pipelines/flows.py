# -*- coding: utf-8 -*-
"""
Imports all flows for every project so we can register all of them.
"""
from pipelines.br_rj_riodejaneiro_brt_gps.flows import *  # noqa
from pipelines.br_rj_riodejaneiro_diretorios.flows import *
from pipelines.br_rj_riodejaneiro_gtfs.flows import *
from pipelines.br_rj_riodejaneiro_onibus_gps.flows import *
from pipelines.br_rj_riodejaneiro_rdo.flows import *
from pipelines.br_rj_riodejaneiro_stpl_gps.flows import *
from pipelines.br_rj_riodejaneiro_stu.flows import *
from pipelines.capture.jae.flows import *  # noqa
from pipelines.capture.templates.flows import *  # noqa
from pipelines.exemplo import *  # noqa
from pipelines.treatment.bilhetagem.flows import *  # noqa
