# -*- coding: utf-8 -*-
# import os

from queries.dev.utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

run_dbt_model(
    dataset_id="tecnologia_servico",
    # table_id="ordem_servico_diaria",
    _vars=vars,
    # flags="--target hmg",
    flags="--target dev",
)


run_dbt_selector(
    selector_name="monitoramento_temperatura", _vars=vars,
    flags = "--target dev --defer --state target-base")

run_dbt_selector(
    selector_name="apuracao_subsidio_v9", _vars=vars,
    flags = "--target dev --defer --state target-base")