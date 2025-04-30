# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta
from typing import Dict, List, Union

from queries.dev.utils import run_dbt_model


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


## materialização do GTFS ##

# feed_list = [
#     "2024-08-16",
#     "2024-08-19",
#     "2024-09-01",
#     "2024-09-13",
#     "2024-09-29",
# ]
# for feed in feed_list:
#     run_dbt_model(
#         dataset_id="gtfs planejamento",
#         exclude="calendario aux_calendario_manual viagem_planejada_planejamento",
#         _vars={
#             "data_versao_gtfs": feed,
#         },
#         flags="--target hmg",
#     )

#     run_dbt_tests(  # ok
#         dataset_id="gtfs planejamento",
#         _vars={"data_versao_gtfs": feed},
#         flags="--target hmg",
#     )

# ## Materialização de sppo_veiculo_dia  ##

run_dbt_model(  # ok
    dataset_id="veiculo",
    table_id="sppo_veiculo_dia",
    upstream=True,
    exclude="+gps_sppo",
    _vars={"start_date": "2024-08-16", "end_date": "2024-10-22"},
    flags="--target hmg",
)
run_dbt_tests(  # ok
    dataset_id="veiculo",
    table_id="sppo_veiculo_dia",
    _vars={"date_range_start": "2024-08-16 00:00:00", "date_range_end": "2024-08-16 00:00:00"},
    flags="--target hmg",
)


# ## Materialização de viagens em D+1 ##

# data_inicial = datetime.strptime("2024-08-17", "%Y-%m-%d")
# data_final = datetime.strptime("2024-10-16", "%Y-%m-%d")

# data_atual = data_inicial
# while data_atual <= data_final:
#     run_dbt_model(
#         dataset_id="projeto_subsidio_sppo",
#         table_id="viagem_completa",
#         upstream=True,
#         exclude="+gps_sppo +ordem_servico_trips_shapes_gtfs +aux_calendario_manual",
#         _vars={"run_date": data_atual.strftime("%Y-%m-%d")},
#         flags="--target hmg",
#     )
#     print(data_atual.strftime("%Y-%m-%d"))
#     data_atual += timedelta(days=1)

# run_dbt_tests(  # ok
#     dataset_id="projeto_subsidio_sppo",
#     table_id="viagem_planejada",
#     _vars={"date_range_start": "2024-08-16", "date_range_end": "2024-10-15"},
#     flags="--target hmg",
# )

# # ## Apuração do Subsídio ##

# run_dbt_model(
#     dataset_id="planejamento.staging",
#     table_id="aux_calendario_manual",
#     _vars={"date_range_start": "2024-08-16", "date_range_end": "2024-10-15"},
#     flags="--target hmg",
# )

run_command = """dbt run --selector apuracao_subsidio_v9 --vars "{'start_date': '2024-08-15', 'end_date': '2024-10-15'}" -x --profiles-dir ./dev --target hmg"""
project_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(project_dir)
os.system(run_command)

run_dbt_tests(  # ok
    dataset_id="dashboard_subsidio_sppo",
    table_id="viagens_remuneradas",
    _vars={"date_range_start": "2024-08-16 00:00:00", "date_range_end": "2024-10-15 00:00:00"},
    flags="--target hmg",
)

run_dbt_tests(  # ok
    dataset_id="dashboard_subsidio_sppo_v2",
    _vars={"date_range_start": "2024-08-16 00:00:00", "date_range_end": "2024-10-15 00:00:00"},
    flags="--target hmg",
)
