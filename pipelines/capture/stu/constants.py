# -*- coding: utf-8 -*-
"""
Valores constantes para captura de dados do STU
"""

from datetime import datetime
from enum import Enum

from pipelines.schedules import create_daily_cron
from pipelines.utils.gcp.bigquery import SourceTable

STU_SOURCE_NAME = "stu"
STU_PRIVATE_BUCKET_NAMES = {"prod": "rj-smtr-stu-private", "dev": "rj-smtr-dev-airbyte"}


class constants(Enum):  # pylint: disable=c0103
    """
    Valores constantes para captura de dados do STU
    """

    STU_COMBUSTIVEL_TABLE_ID = "combustivel"
    STU_CONTROLE_PROCESSO_TABLE_ID = "controle_processo"
    STU_DARM_APROPRIACAO_TABLE_ID = "darm_apropriacao"
    STU_MOD_CARROCERIA_TABLE_ID = "mod_carroceria"
    STU_MOD_CHASSI_TABLE_ID = "mod_chassi"
    STU_MULTA_TABLE_ID = "multa"
    STU_PERMISSAO_TABLE_ID = "permissao"
    STU_PESSOA_FISICA_TABLE_ID = "pessoa_fisica"
    STU_PESSOA_JURIDICA_TABLE_ID = "pessoa_juridica"
    STU_PLANTA_TABLE_ID = "planta"
    STU_TALONARIO_TABLE_ID = "talonario"
    STU_TIPO_DE_PERMISSAO_TABLE_ID = "tipo_de_permissao"
    STU_TIPO_DE_PERMISSIONARIO_TABLE_ID = "tipo_de_permissionario"
    STU_TIPO_DE_TRANSPORTE_TABLE_ID = "tipo_de_transporte"
    STU_TIPO_DE_VEICULO_TABLE_ID = "tipo_de_veiculo"
    STU_VEICULO_TABLE_ID = "veiculo"
    STU_VEICULO_ATIVO_TABLE_ID = "veiculo_ativo"
    STU_VISTORIA_TABLE_ID = "vistoria"
    STU_GUIA_TABLE_ID = "guia"
    STU_PERMIS_EMPRESA_ESCOLA_TABLE_ID = "permis_empresa_escola"
    STU_MODELO_TABLE_ID = "modelo"

    STU_TABLE_CAPTURE_PARAMS = {
        STU_COMBUSTIVEL_TABLE_ID: {
            "primary_keys": ["cod_combustivel"],
        },
        STU_CONTROLE_PROCESSO_TABLE_ID: {
            "primary_keys": ["Id"],
        },
        STU_DARM_APROPRIACAO_TABLE_ID: {
            "primary_keys": ["codrec", "anodarm", "darm", "Emissao"],
        },
        STU_MOD_CARROCERIA_TABLE_ID: {
            "primary_keys": ["cod_mod_carroceria"],
        },
        STU_MOD_CHASSI_TABLE_ID: {
            "primary_keys": ["cod_fab_chassi"],
        },
        STU_MULTA_TABLE_ID: {
            "primary_keys": ["serie", "cm"],
        },
        STU_PERMISSAO_TABLE_ID: {
            "primary_keys": ["tptran", "tpperm", "termo", "dv"],
        },
        STU_PESSOA_FISICA_TABLE_ID: {
            "primary_keys": ["ratr"],
        },
        STU_PESSOA_JURIDICA_TABLE_ID: {
            "primary_keys": ["cgc"],
        },
        STU_PLANTA_TABLE_ID: {
            "primary_keys": ["planta"],
        },
        STU_TALONARIO_TABLE_ID: {
            "primary_keys": ["serie", "cm"],
        },
        STU_TIPO_DE_PERMISSAO_TABLE_ID: {
            "primary_keys": ["tptran", "tpperm"],
        },
        STU_TIPO_DE_PERMISSIONARIO_TABLE_ID: {
            "primary_keys": ["tpperm"],
        },
        STU_TIPO_DE_TRANSPORTE_TABLE_ID: {
            "primary_keys": ["tptran"],
        },
        STU_TIPO_DE_VEICULO_TABLE_ID: {
            "primary_keys": ["tpveic"],
        },
        STU_VEICULO_TABLE_ID: {
            "primary_keys": ["placa"],
        },
        STU_VEICULO_ATIVO_TABLE_ID: {
            "primary_keys": ["placa"],
        },
        STU_VISTORIA_TABLE_ID: {
            "primary_keys": ["id_vistoria", "anoexe"],
        },
        STU_GUIA_TABLE_ID: {
            "primary_keys": ["guia"],
            "first_timestamp": datetime(2025, 12, 5, 0, 0, 0),
        },
        STU_PERMIS_EMPRESA_ESCOLA_TABLE_ID: {
            "primary_keys": ["tptran", "tpperm", "termo"],
            "first_timestamp": datetime(2025, 12, 5, 0, 0, 0),
        },
        STU_MODELO_TABLE_ID: {
            "primary_keys": ["cod_modelo"],
            "first_timestamp": datetime(2025, 12, 5, 0, 0, 0),
        },
    }

    STU_SOURCES = [
        SourceTable(
            source_name=STU_SOURCE_NAME,
            table_id=k,
            first_timestamp=v.get("first_timestamp", datetime(2025, 10, 9, 0, 0, 0)),
            schedule_cron=create_daily_cron(hour=8),
            primary_keys=v["primary_keys"],
            pretreatment_reader_args=v.get("pretreatment_reader_args"),
            pretreat_funcs=v.get("pretreat_funcs"),
            bucket_names=STU_PRIVATE_BUCKET_NAMES,
            partition_date_only=True,
            raw_filetype=v.get("raw_filetype", "csv"),
        )
        for k, v in STU_TABLE_CAPTURE_PARAMS.items()
    ]
