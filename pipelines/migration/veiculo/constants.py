# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr veiculo
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr veiculo
    """

    SPPO_LICENCIAMENTO_TABLE_ID = "licenciamento_stu"

    SPPO_LICENCIAMENTO_MAPPING_KEYS = {
        "placa": "placa",
        "ordem": "id_veiculo",
        "permissao": "permissao",
        "modal": "modo",
        "ultima_vistoria": "data_ultima_vistoria",
        "cod_planta": "id_planta",
        "cod_mod_carroceria": "id_carroceria",
        "cod_fab_carroceria": "id_interno_carroceria",
        "des_mod_carroceria": "carroceria",
        "cod_mod_chassi": "id_chassi",
        "cod_fab_chassi": "id_fabricante_chassi",
        "des_mod_chassi": "nome_chassi",
        "lotacao_sentado": "quantidade_lotacao_sentado",
        "lotacao_pe": "quantidade_lotacao_pe",
        "elevador": "indicador_elevador",
        "ar_condicionado": "indicador_ar_condicionado_stu",
        "tipo_veiculo": "tipo_veiculo",
        "combustivel": "tipo_combustivel",
        "portas": "quantidade_portas",
        "ano_fabricacao": "ano_fabricacao",
        "wifi": "indicador_wifi",
        "usb": "indicador_usb",
        "data_inicio_vinculo": "data_inicio_vinculo",
        "ultima_situacao": "ultima_situacao",
        "ano_ultima_vistoria": "ano_ultima_vistoria",
    }

    SPPO_LICENCIAMENTO_CSV_ARGS = {
        "sep": ";",
        "names": SPPO_LICENCIAMENTO_MAPPING_KEYS.keys(),  # pylint: disable=e1101
    }

    SPPO_INFRACAO_TABLE_ID = "infracao"

    SPPO_INFRACAO_MAPPING_KEYS = {
        "permissao": "permissao",
        "modal": "modo",
        "placa": "placa",
        "cm": "id_auto_infracao",
        "data_infracao": "data_infracao",
        "valor": "valor",
        # "datetime_infracao": "datetime_infracao",
        "cod_infracao": "id_infracao",
        "des_infracao": "infracao",
        "status": "status",
        "data_pagamento": "data_pagamento",
        "linha": "servico",
    }
    SPPO_INFRACAO_CSV_ARGS = {
        "sep": ";",
        "names": SPPO_INFRACAO_MAPPING_KEYS.keys(),  # pylint: disable=e1101
    }

    SPPO_VEICULO_DIA_TABLE_ID = "sppo_veiculo_dia"

    # AUTUAÇÕES - AGENTES DE VERÃO
    SPPO_REGISTRO_AGENTE_VERAO_COLUMNS = [
        "datetime_registro",
        "email",
        "id_veiculo",
        "servico",
        "link_foto",
        "validacao",
    ]

    SPPO_REGISTRO_AGENTE_VERAO_PARAMS = {
        "partition_date_only": True,
        "source_type": "api-csv",
        "dataset_id": smtr_constants.VEICULO_DATASET_ID.value,
        "table_id": "sppo_registro_agente_verao",
        "extract_params": {"secret_path": "smtr_agentes_verao"},
        "pre_treatment_reader_args": {
            "skiprows": 2,
            "names": SPPO_REGISTRO_AGENTE_VERAO_COLUMNS,
        },
        "primary_key": ["datetime_registro", "email"],
    }

    VEICULO_DATA_QUALITY_CHECK_LIST = {
        "infracao": {
            "dbt_expectations.expect_table_aggregation_to_equal_other_table__infracao": {
                "description": "Todas as datas possuem dados"
            }
        },
        "sppo_veiculo_dia": {
            "not_null": {"description": "Todos os valores da coluna `{column_name}` não nulos"},
            "dbt_utils.unique_combination_of_columns__data_id_veiculo": {
                "description": "Todos os registros são únicos"
            },
            "dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart": {
                "description": "Todas as datas possuem dados"
            },
        },
    }
