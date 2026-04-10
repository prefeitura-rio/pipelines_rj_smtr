
from queries.dev.utils import run_dbt, run_dbt_selector, run_dbt_tests, run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py

import pandas as pd

# inicio = "2025-07-01"
# fim = "2025-07-31"
# # Criar range de datas
inicio = "2026-03-01"
fim = "2026-03-15"
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
    }


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