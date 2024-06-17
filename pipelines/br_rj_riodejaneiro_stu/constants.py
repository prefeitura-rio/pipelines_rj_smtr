# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr controle_financeiro
"""

from enum import Enum

from pipelines.constants import constants as smtr_constants


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr br_rj_riodejaneiro_stu
    """

    STU_GENERAL_CAPTURE_PARAMS = {
        "partition_date_only": True,
        "source_type": "gcs",
        "dataset_id": smtr_constants.STU_DATASET_ID.value,
        "save_bucket_name": smtr_constants.STU_BUCKET_NAME.value,
    }

    STU_TABLE_CAPTURE_PARAMS = [
        {
            "table_id": "operadora_empresa",
            "primary_key": ["Perm_Autor"],
            "pre_treatment_reader_args": {"dtype": "object"},
        },
        {
            "table_id": "operadora_pessoa_fisica",
            "primary_key": ["Perm_Autor"],
            "pre_treatment_reader_args": {"dtype": "object"},
        },
    ]

    STU_MODE_MAPPING = {
        "1": "Táxi",
        "2": "Ônibus",
        "3": "Escolar",
        "4": "Complementar (cabritinho)",
        "6": "Fretamento",
        "7": "TEC",
        "8": "Van",
    }

    STU_TYPE_MAPPING = [
        "Autônomo",
        "Empresa",
        "Cooperativa",
        "Instituicao de Ensino",
        "Associações",
        "Autônomo Provisório",
        "Contrato Público",
        "Prestadora de Serviços",
    ]
