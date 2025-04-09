# -*- coding: utf-8 -*-
"""
Imports all flows for every project so we can register all of them.
"""
from pipelines.capture.jae.flows import *  # noqa
from pipelines.capture.rioonibus.flows import *  # noqa
from pipelines.capture.serpro.flows import *  # noqa
from pipelines.capture.sonda.flows import *  # noqa
from pipelines.control.flows import *  # noqa
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
from pipelines.treatment.bilhetagem.flows import *  # noqa
from pipelines.treatment.cadastro.flows import *  # noqa
from pipelines.treatment.datario.flows import *  # noqa
from pipelines.treatment.financeiro.flows import *  # noqa
from pipelines.treatment.monitoramento.flows import *  # noqa
from pipelines.treatment.planejamento.flows import *  # noqa
from pipelines.treatment.transito.flows import *  # noqa
from pipelines.treatment.validacao_dados_jae.flows import *  # noqa
