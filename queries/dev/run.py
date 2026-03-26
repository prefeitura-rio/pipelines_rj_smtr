# -*- coding: utf-8 -*-
# import os

from queries.dev.utils import run_dbt_model, run_dbt_selector, run_dbt_tests
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
    "data_versao_gtfs": "2026-01-26",
    "feed_start_date": "2026-01-26",
    
    }


# run_dbt_model(
# dataset_id="ordem_servico_trips_shapes_gtfs",
# _vars= vars,
#     flags = "--target hmg --defer --state target-base --favor-state")





# run_dbt_model(
# dataset_id="viagem_transacao",
# _vars= vars,
#     flags = "--target hmg --defer --state target-base --favor-state")


# run_dbt_selector(
#     selector_name="planejamento_diario",     
#     _vars=vars, 
#     flags = "--target dev --defer --state target-base")

run_dbt_selector(
    selector_name="planejamento_diario",     
    _vars=vars, 
    flags = "--target hmg --defer --state target-base")



# for date in run_dates:
#     vars = {"run_date": date.strftime("%Y-%m-%d")}
#     run_dbt_model(
#     dataset_id="viagem_completa",
#     upstream= True,
#     exclude= "+gps_sppo +gtfs +subsidio_shapes_geom",
#     _vars= vars,
#     flags = "--target dev --defer --state target-base")

# for date in run_dates:
#     vars = {"run_date": date.strftime("%Y-%m-%d")}
#     run_dbt_model(
#     dataset_id="subsidio_data_versao_efetiva",
#     _vars= vars,
#     flags = "--target dev --defer --state target-base")

# for date in run_dates:
#     vars = {"run_date": date.strftime("%Y-%m-%d")}
#     run_dbt_model(
#     dataset_id="viagem_completa",
#     upstream= True,
#     exclude= "+gps_sppo +gtfs +ordem_servico_trips_shapes",
#     _vars= vars,
#     flags = "--target dev --defer --state target-base --favor-state")







# run_dbt_selector(
#     selector_name="monitoramento_temperatura",     
#     _vars=vars, 
#     flags = "--target hmg --defer --state target-base")

# # pré-testes
# run_dbt_tests(
#     dataset_id="tecnologia_servico viagem_planejada",
#       _vars=vars, 
#     flags = "--target hmg --defer --state target-base")


# run_dbt_selector(
#     selector_name="apuracao_subsidio_v9", 
#     _vars=vars,
#     flags = "--target hmg --defer --state target-base")



# run_dbt_model(
# dataset_id="viagem_regularidade_temperatura",
# _vars= vars,
#     flags = "--target dev --defer --state target-base --favor-state")




# #pós-testes
# run_dbt_tests(
#     dataset_id="viagem_regularidade_temperatura",
#       _vars=vars, 
#     flags = "--target dev --defer --state target-base")
