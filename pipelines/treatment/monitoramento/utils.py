# -*- coding: utf-8 -*-
from pipelines.constants import constants as smtr_constants
from pipelines.treatment.monitoramento.constants import constants
from pipelines.treatment.templates.flows import create_default_materialization_flow
from pipelines.utils.prefect import set_default_parameters


def create_gps_materialization_flow(
    modo_gps: str,
    fonte_gps: str,
    wait_sources: list,
) -> tuple:
    """
    Cria flows de materialização de GPS para uma fonte específica.

    Args:
        modo_gps (str): Modo do GPS (ex: "onibus")
        fonte_gps (str): Fonte do GPS (ex: "conecta", "cittati", "zirix")
        wait_sources (list): Lista de fontes de dados para aguardar

    Returns:
        tuple: (flow_gps_normal, flow_gps_15_minutos)
    """

    gps_flow = create_default_materialization_flow(
        flow_name=f"gps {fonte_gps} - materializacao",
        selector=constants.GPS_SELECTOR.value,
        agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
        wait=wait_sources,
    )

    gps_vars = {"modo_gps": modo_gps, "fonte_gps": fonte_gps, "15_minutos": False}
    set_default_parameters(gps_flow, {"additional_vars": gps_vars})

    gps_15_minutos_flow = create_default_materialization_flow(
        flow_name=f"gps_15_minutos {fonte_gps} - materializacao",
        selector=constants.GPS_15_MINUTOS_SELECTOR.value,
        agent_label=smtr_constants.RJ_SMTR_AGENT_LABEL.value,
        wait=wait_sources,
    )

    gps_15_vars = {"modo_gps": modo_gps, "fonte_gps": fonte_gps, "15_minutos": True}
    set_default_parameters(gps_15_minutos_flow, {"additional_vars": gps_15_vars})

    return gps_flow, gps_15_minutos_flow
