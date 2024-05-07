# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados da Stu
"""

from enum import Enum


class constants(Enum):
    """
    Valores constantes para captura de dados da Stu
    """

    STU_SOURCE_NAME = "stu"

    STU_DATASET_ID = "br_rj_riodejaneiro_stu"

    STU_BUCKET_NAME = {"dev": "br-rj-smtr-stu-private-dev", "prod": "br-rj-smtr-stu-private"}

    STU_PREFIX_TIPO_CADASTRO = {
        "operadora_pessoa_fisica": [
            "Tptran_1_Tpperm_1",
            "Tptran_1_Tpperm_6",
            "Tptran_3_Tpperm_1",
            "Tptran_4_Tpperm_1",
            "Tptran_4_Tpperm_6",
            "Tptran_6_Tpperm_1",
            "Tptran_6_Tpperm_8",
            "Tptran_7_Tpperm_6",
            "Tptran_8_Tpperm_1",
        ],
        "operadora_empresa": [
            "Tptran_1_Tpperm_2",
            "Tptran_2_Tpperm_2",
            "Tptran_3_Tpperm_2",
            "Tptran_3_Tpperm_4",
            "Tptran_6_Tpperm_2",
        ],
    }

    STU_GENERAL_CAPTURE_PARAMS = {
        "save_bucket_name": STU_BUCKET_NAME,
        "primary_keys": ["Perm_Autor"],
        "pre_treatment_reader_args": {"dtype": "object"},
        "raw_filetype": "txt",
    }

    STU_CAPTURE_PARAMS = [
        {
            "table_id": "operadora_empresa",
        },
        {
            "table_id": "operadora_pessoa_fisica",
        },
    ]
