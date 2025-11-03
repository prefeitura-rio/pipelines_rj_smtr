# -*- coding: utf-8 -*-
# import os

from queries.dev.utils import run_dbt_model, run_dbt_tests, run_dbt_selector

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

inicio = "2025-10-21"
fim = "2025-10-21"

# run_dbt_selector(
#     selector_name="monitoramento_temperatura", _vars={"start_date": inicio, "end_date": fim},
#     flags = "--target dev --defer --state target-base --favor-state")

run_dbt_tests(
    dataset_id="temperatura",
      _vars={"date_range_start": inicio, "date_range_end": fim}, 
    flags = "--target dev --defer --state target-base ")