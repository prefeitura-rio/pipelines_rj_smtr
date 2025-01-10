# -*- coding: utf-8 -*-
"""
Constant values for rj_smtr serpro
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for rj_smtr serpro
    """

    INFRACAO_DATASET_ID = "infracao"
    AUTUACAO_SERPRO_TABLE_ID = "autuacao_serpro"
    AUTUACAO_MATERIALIZACAO_DATASET_ID = "transito"
    AUTUACAO_MATERIALIZACAO_TABLE_ID = "autuacao"

    INFRACAO_PRIVATE_BUCKET = "rj-smtr-infracao-private"

    SERPRO_CAPTURE_PARAMS = {
        "query": """
            SELECT
                *
            FROM
                dbpro_radar_view_SMTR_VBL.tb_infracao_view
            WHERE
                PARSEDATE(SUBSTRING(auinf_dt_infracao, 1, 10), 'yyyy-MM-dd')
                BETWEEN PARSEDATE('{start_date}', 'yyyy-MM-dd')
                AND PARSEDATE('{end_date}', 'yyyy-MM-dd')
        """,
        "primary_key": ["auinf_num_auto"],
        "pre_treatment_reader_args": {"dtype": "object"},
    }
