# -*- coding: utf-8 -*-
# import os
import pandas as pd

from queries.dev.utils import run_dbt, run_dbt_model, run_dbt_selector, run_dbt_tests

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

inicio = "2025-04-01"
fim = "2026-03-15"
run_dates = pd.date_range(start=inicio, end=fim, freq="D")
run_dates_list = run_dates.strftime("%Y-%m-%d").tolist()
partitions = ", ".join([f"date({dt.year}, {dt.month}, {dt.day})" for dt in run_dates])
vars = {
    "start_date": inicio,
    "end_date": fim,
    "date_range_start": inicio + "T00:00:00",
    "date_range_end": fim + "T01:00:00",
    # "partitions": partitions,
}

# run_dbt_model(
#     # selector_name="apuracao_subsidio_v9",
#     dataset_id="viagens_remuneradas",
#     _vars=vars,
#     # flags="--target hmg",
#     flags="--target dev --defer --state",
# )
run_dbt(
    resource="model",
    selector_name="apuracao_subsidio_v9",
    _vars=vars,
    # flags="--target hmg",
    flags="--target dev --defer --state target-base --favor-state",
)

run_dbt_tests(
    dataset_id="viagem_classificada viagem_regularidade_temperatura viagens_remuneradas sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem",
    # dataset_id="viagens_remuneradas sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem",
    _vars=vars,
    exclude="aux_viagem_temperatura veiculo_regularidade_temperatura_dia",
    flags="--target dev --defer --state target-base",
)
