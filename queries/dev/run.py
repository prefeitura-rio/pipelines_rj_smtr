# -*- coding: utf-8 -*-
# import os

from queries.dev.utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py
inicio = "2026-01-27"
fim = "2026-01-27"
run_dbt_model(
dataset_id="viagem_classificada",
_vars={"start_date": inicio, "date_range_start":inicio,  "end_date": fim, "date_range_end": fim},
    flags = "--target dev --defer --state target-base --favor-state")