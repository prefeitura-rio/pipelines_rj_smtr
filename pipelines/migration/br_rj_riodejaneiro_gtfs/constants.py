# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr br_rj_riodejaneiro_gtfs
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_gtfs
    """

    GTFS_DATA_CHECKS_LIST = {
        "calendar_gtfs": {
            "dbt_expectations.expect_column_values_to_match_regex__service_id__calendar_gtfs": {
                "description": "Todos os 'service\\_id' começam com 'U\\_', 'S\\_', 'D\\_' ou 'EXCEP'."  # noqa
            },
        },
        "ordem_servico_trajeto_alternativo_gtfs": {
            "dbt_expectations.expect_table_aggregation_to_equal_other_table__ordem_servico_trajeto_alternativo_gtfs": {  # noqa
                "description": "Todos os dados de 'feed_start_date' e 'tipo_os' correspondem 1:1 entre as tabelas 'ordem_servico_trajeto_alternativo_gtfs' e 'ordem_servico_gtfs'."  # noqa
            },
        },
        "ordem_servico_trajeto_alternativo_sentido": {
            "dbt_expectations.expect_table_aggregation_to_equal_other_table__ordem_servico_trajeto_alternativo_sentido": {  # noqa
                "description": "Todos os dados de 'feed_start_date' e 'tipo_os' correspondem 1:1 entre as tabelas 'ordem_servico_trajeto_alternativo_sentido' e 'ordem_servico_gtfs'."  # noqa
            },
        },
        "ordem_servico_trips_shapes_gtfs": {
            "dbt_expectations.expect_table_aggregation_to_equal_other_table__ordem_servico_trips_shapes_gtfs": {  # noqa
                "description": "Todos os dados de 'feed_start_date', 'tipo_os', 'tipo_dia', 'servico' e 'faixa_horaria_inicio' correspondem 1:1 entre as tabelas 'ordem_servico_trips_shapes_gtfs' e 'ordem_servico_faixa_horaria'."  # noqa
            },
            "dbt_utils.unique_combination_of_columns__ordem_servico_trips_shapes_gtfs": {
                "description": "Todos os dados de 'feed_start_date', 'tipo_dia', 'tipo_os', 'servico', 'sentido', 'faixa_horaria_inicio' e 'shape_id' são únicos."  # noqa
            },
            "dbt_expectations.expect_table_row_count_to_be_between__ordem_servico_trips_shapes_gtfs": {  # noqa
                "description": "A quantidade de registros de 'feed_start_date', 'tipo_dia', 'tipo_os', 'servico', 'faixa_horaria_inicio' e 'shape_id' está dentro do intervalo esperado."  # noqa
            },
        },
        "trips_gtfs": {
            "test_shape_id_gtfs__trips_gtfs": {
                "description": "Todos os `shape_id` de `trips_gtfs` constam na tabela `shapes_gtfs`"
            },
        }
    }