# -*- coding: utf-8 -*-
"""
Imports all flows for every project so we can register all of them.
"""
from pipelines.capture.rioonibus.flows import *  # noqa
from pipelines.exemplo import *  # noqa
from pipelines.janitor.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_bilhetagem.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_brt_gps.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_diretorios.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_gtfs.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_onibus_gps.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_onibus_gps_zirix.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_rdo.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_recursos.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_stpl_gps.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_stu.flows import *  # noqa
from pipelines.migration.br_rj_riodejaneiro_viagem_zirix.flows import *  # noqa
from pipelines.migration.controle_financeiro.flows import *  # noqa
from pipelines.migration.projeto_subsidio_sppo.flows import *  # noqa
from pipelines.migration.veiculo.flows import *  # noqa
from pipelines.serpro.flows import *  # noqa
from pipelines.treatment.monitoramento.flows import *  # noqa
