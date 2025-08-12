# -*- coding: utf-8 -*-
# import os

from queries.dev.utils import run_dbt_model, run_dbt_tests

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

# run_dbt_model(
#     dataset_id="viagem_regularidade_temperatura",
#     # table_id="my_first_dbt_model",
# )

run_dbt_tests(
    dataset_id="test_check_veiculo_lacre__veiculo_dia",
    # table_id="ordem_servico_trips_shapes_gtfs",
    # upstream=True,
    # exclude="aux_segmento_shape+ ordem_servico_faixa_horaria tecnologia_servico sumario_faixa_servico_dia sumario_faixa_servico_dia_pagamento viagem_planejada viagens_remuneradas sumario_servico_dia_historico",
    _vars={"date_range_start": "2025-07-16", "date_range_end": "2025-07-21"},
    flags="--target prod",
)
