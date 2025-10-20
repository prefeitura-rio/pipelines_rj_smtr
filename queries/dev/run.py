# -*- coding: utf-8 -*-
# import os

from queries.dev.utils import run_dbt, run_dbt_selector, run_dbt_tests, run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py



run_dbt_model(
dataset_id="veiculo_licenciamento_dia",
_vars={"start_date": "2025-04-01", "date_range_start":"2025-04-01",  "end_date": "2025-04-15", "date_range_end": "2025-04-15"},
    flags = "--target dev --defer --state target-base --favor-state")





# run_dbt_selector(
#     selector_name="cadastro_veiculo", _vars={"start_date": "2025-04-01", 
#                                                   "date_range_start":"2025-04-01",  "end_date": "2025-04-15", "date_range_end": "2025-04-15"},
#     flags = "--target dev --defer --state target-base --favor-state")


# run_dbt_selector(
#     selector_name="monitoramento_veiculo", _vars={"start_date": "2025-04-01", 
#                                                   "date_range_start":"2025-04-01",  "end_date": "2025-04-15", "date_range_end": "2025-04-15"},
#     flags = "--target dev --defer --state target-base --favor-state")

# run_dbt_selector(
#     selector_name="apuracao_subsidio_v9", _vars={"start_date": "2025-04-01", "end_date": "2025-04-15"},
#     flags = "--target dev --defer --state target-base --favor-state")





# run_dbt_tests(
#     dataset_id="viagem_classificada viagem_regularidade_temperatura viagens_remuneradas sumario_faixa_servico_dia_pagamento valor_km_tipo_viagem",
#       _vars={"date_range_start": "2025-06-01", "date_range_end": "2025-05-30"}, 
#     flags = "--target hmg --defer --state target-base ")



# run_dbt_tests(
#     dataset_id="test_consistencia_indicadores_temperatura__viagem_regularidade_temperatura",
#       _vars={"date_range_start": "2025-09-01", "date_range_end": "2025-09-01"}, 
#     flags = "--target hmg --defer --state target-base ")


