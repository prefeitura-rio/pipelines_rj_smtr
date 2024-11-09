# -*- coding: utf-8 -*-
import os
from typing import Dict, List, Union


def run_dbt_tests(
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
):
    """
    Run DBT test
    """
    run_command = "dbt test"

    common_flags = "--profiles-dir ./dev"

    if flags:
        flags = f"{common_flags} {flags}"
    else:
        flags = common_flags

    if not model:
        model = dataset_id
        if table_id:
            model += f".{table_id}"

    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += model
        if downstream:
            run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    print(f"\n>>> RUNNING: {run_command}\n")

    project_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    os.chdir(project_dir)
    os.system(run_command)


from utils import run_dbt_model
from datetime import datetime, timedelta

# ordem_servico_trips_shapes_gtfs ##
run_dbt_model(
    dataset_id="gtfs",
    # dataset_id="planejamento",
    table_id="ordem_servico_trips_shapes_gtfs",
    # upstream=True,
    _vars={"data_versao_gtfs": "2024-11-06"},
    flags="--target hmg",
)

## dados de autuação ##
# run_dbt_model(
#     dataset_id="transito",
#     table_id="receita_autuacao",
#     # upstream=True,
#     # _vars={"date_range_start": "2019-01-01", "date_range_end": "2023-08-26"},
#     flags="--full-refresh",
# )

# dados viagens ##
# run_dbt_model(
#     dataset_id="projeto_subsidio_sppo",
#     table_id="viagem_completa",
#     upstream=True,
#     exclude="+gps_sppo +ordem_servico_trips_shapes_gtfs",
#     _vars={"run_date": "2024-11-07"},
#     flags="--target hmg",
# )

## loop para dados de viagens em D+1 ##

# data_inicial = datetime.strptime("2024-11-07", "%Y-%m-%d")
# data_final = datetime.strptime("2024-11-08", "%Y-%m-%d")

# data_atual = data_inicial
# while data_atual <= data_final:
#     run_dbt_model(
#         dataset_id="projeto_subsidio_sppo",
#         table_id="viagem_completa",
#         upstream=True,
#         exclude="+gps_sppo +ordem_servico_trips_shapes_gtfs",
#         _vars={"run_date": data_atual.strftime("%Y-%m-%d")},
#         # flags="--full-refresh",
#         flags="--target hmg",
#     )
#     print(data_atual.strftime("%Y-%m-%d"))
#     data_atual += timedelta(days=1)

# data_inicial = datetime.strptime("2024-10-02", "%Y-%m-%d")
# data_final = datetime.strptime("2024-10-16", "%Y-%m-%d")

# data_atual = data_inicial
# while data_atual <= data_final:
#     run_dbt_model(
#         dataset_id="projeto_subsidio_sppo",
#         table_id="viagem_completa",
#         upstream=True,
#         exclude="+gps_sppo +ordem_servico_trips_shapes_gtfs",
#         _vars={"run_date": data_atual.strftime("%Y-%m-%d")},
#         # flags="--full-refresh",
#         flags="--target hmg",
#     )
#     print(data_atual.strftime("%Y-%m-%d"))
#     data_atual += timedelta(days=1)

## dados subsidio ##
# run_dbt_model(
#     dataset_id=" subsidio dashboard_subsidio_sppo",
#     _vars={"start_date": "2024-07-16", "end_date": "2024-07-31"},
#     # flags="--full-refresh",
# )
# run_dbt_model(
#     dataset_id="dashboard_subsidio_sppo",
#     table_id="sumario_servico_dia_historico",
#     _vars={"start_date": "2024-07-16", "end_date": "2024-07-17"},
#     # flags="--full-refresh",
# )
# run_dbt_model(
#     dataset_id="subsidio",
#     # table_id="sumario_servico_dia_tipo_sem_glosa",
#     _vars={"start_date": "2024-07-20", "end_date": "2024-07-20"},
#     flags="--full-refresh",
# )


## Teste de modelos ##
# run_dbt_tests(  # ok
#     dataset_id="br_rj_riodejaneiro_onibus_gps",
#     table_id="sppo_registros sppo_realocacao",
#     _vars={"start_timestamp": "2024-09-01 00:00:00", "end_timestamp": "2024-09-15 03:00:00"},
#     flags="--target hmg",
# )
# run_dbt_tests(  # ok
#     dataset_id="br_rj_riodejaneiro_veiculos",
#     table_id="gps_sppo",
#     _vars={"start_timestamp": "2024-09-01 00:00:00", "end_timestamp": "2024-09-15 03:00:00"},
#     flags="--target dev",
# )
# run_dbt_tests(  # ok
#     dataset_id="veiculo",
#     table_id="sppo_veiculo_dia",
#     _vars={"start_timestamp": "2024-09-01 00:00:00", "end_timestamp": "2024-09-15 00:00:00"},
#     flags="--target dev",
# )
# run_dbt_tests(  # ok
#     dataset_id="dashboard_subsidio_sppo",
#     table_id="viagens_remuneradas",
#     _vars={"start_timestamp": "2024-10-06 00:00:00", "end_timestamp": "2024-10-06 03:00:00"},
#     flags="--target hmg",
# )
# run_dbt_tests(  # ok
#     dataset_id="dashboard_subsidio_sppo_v2",
#     table_id="sumario_servico_dia_pagamento",
#     _vars={"start_timestamp": "2024-10-06 00:00:00", "end_timestamp": "2024-10-06 03:00:00"},
#     flags="--target hmg",
# )
# run_dbt_tests(  # ok
#     dataset_id="dashboard_subsidio_sppo",
#     table_id="sumario_servico_dia_historico",
#     _vars={"start_timestamp": "2024-09-01 00:00:00", "end_timestamp": "2024-09-15 00:00:00"},
#     flags="--target dev",
# )
# run_dbt_tests( # ok
#     dataset_id="dashboard_subsidio_sppo",
#     table_id="sumario_servico_dia",
#     _vars={"start_timestamp": "2024-07-19 00:00:00", "end_timestamp": "2024-07-20 00:00:00"},
# )
# run_dbt_tests( # ok
#     dataset_id="dashboard_subsidio_sppo",
#     table_id="sumario_servico_dia_tipo_sem_glosa",
#     _vars={"start_timestamp": "2024-07-19 00:00:00", "end_timestamp": "2024-07-19 00:00:00"},
# )
# run_dbt_tests(  # ok
#     dataset_id="dashboard_subsidio_sppo",
#     table_id="sumario_servico_dia_tipo",
#     _vars={"start_timestamp": "2024-07-19 00:00:00", "end_timestamp": "2024-07-19 00:00:00"},
# )


## Selector apuração ##
# run_command = """dbt run --selector apuracao_subsidio_v9 --vars "{'start_date': '2024-10-01', 'end_date': '2024-10-05'}" -x --profiles-dir ./dev --target hmg"""

# project_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
# os.chdir(project_dir)
# os.system(run_command)

# run_command = """dbt run --selector apuracao_subsidio_v9 --vars "{'start_date': '2024-10-06', 'end_date': '2024-10-06'}" -x --profiles-dir ./dev --target hmg"""

# project_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
# os.chdir(project_dir)
# os.system(run_command)
