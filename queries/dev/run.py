# -*- coding: utf-8 -*-
# import os

from queries.dev.utils import run_dbt_model, run_dbt_tests, run_dbt_selector
import pandas as pd
# Veja os parâmetros disponíveis da função run_dbt_model em util.py

inicio = "2026-02-01"
fim = "2026-02-15"
# Criar range de datas
run_dates = pd.date_range(start=inicio, end=fim, freq="D")
# Montar string de partitions
partitions = ", ".join([f"date({dt.year}, {dt.month}, {dt.day})" for dt in run_dates])
# # Variáveis para passar ao dbt
vars = {
    "start_date": inicio,
    "end_date": fim,
    "date_range_start": f"{inicio}T00:00:00",
    "date_range_end": f"{fim}T23:59:59",
    "partitions": partitions,
    "data_versao_gtfs": "2025-06-01",
    "feed_start_date": "2025-06-01",
    }


# run_dbt_model(
# dataset_id="aux_viagem_temperatura",
# _vars= vars,
#     flags = "--target dev --defer --state target-base")

# run_dbt_selector(
#     selector_name="monitoramento_temperatura", _vars=vars,
#     flags = "--target dev --defer --state target-base --favor-state")


run_dbt_selector(
    selector_name="apuracao_subsidio_v9", _vars=vars,
    flags = "--target dev --defer --state target-base")



# run_dbt_tests(
#     dataset_id="viagem_classificada viagem_regularidade_temperatura viagens_remuneradas sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem",
#       _vars=vars, 
#     flags = "--target dev")