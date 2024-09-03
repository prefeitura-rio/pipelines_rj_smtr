-- models/smtr_autuacoes.sql

{{ config(
    materialized='incremental',
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    unique_key='id_autuacao',
    incremental_strategy='insert_overwrite'
) }}

WITH citran AS (

    select
        DATE(data)  data,
        id_auto_infracao,
        TIMESTAMP(concat(data,' ',hora,':00')) AS datetime_autuacao,
        data_limite_defesa_previa,
        data_limite_recurso,
        NULL AS descricao_situacao_autuacao,
        status_infracao,
        codigo_enquadramento,
        tipificacao_resumida,
        pontuacao,
        NULL AS gravidade,
        NULL AS amparo_legal,
        tipo_veiculo,
        descricao_veiculo,
        NULL AS placa_veiculo,
        NULL AS ano_fabricacao_veiculo,
        NULL AS ano_modelo_veiculo,
        NULL AS cor_veiculo,
        especie_veiculo,
        NULL AS uf_infrator,
        NULL AS uf_principal_condutor,
        uf_proprietario,
        cep_proprietario,
        valor_infracao,
        valor_pago,
        data_pagamento,
        id_autuador,
        descricao_autuador,
        id_municipio_autuacao,
        descricao_municipio,
        "RJ" AS uf_autuacao,
        NULL AS cep_autuacao, -- não padronizado na citran
        NULL AS tile_autuacao,
        processo_defesa_autuacao,
        recurso_penalidade_multa,
        processo_troca_real_infrator,
        status_sne,
        "CITRAN" AS fonte
    FROM {{ ref('autuacoes_citran') }}
    {% if is_incremental() %}
        WHERE
            data BETWEEN DATE("{var('date_range_start')}") AND DATE("{var('date_range_end')}")
    {% endif %}
)

SELECT
    data,
    GENERATE_UUID() AS id_autuacao,
    id_auto_infracao,
    datetime_autuacao,
    data_limite_defesa_previa,
    data_limite_recurso,
    descricao_situacao_autuacao,
    status_infracao,
    codigo_enquadramento,
    tipificacao_resumida,
    pontuacao,
    gravidade,
    amparo_legal,
    tipo_veiculo,
    descricao_veiculo,
    placa_veiculo,
    ano_fabricacao_veiculo,
    ano_modelo_veiculo,
    cor_veiculo,
    especie_veiculo,
    uf_infrator,
    uf_principal_condutor,
    uf_proprietario,
    cep_proprietario,
    valor_infracao,
    valor_pago,
    data_pagamento,
    id_autuador,
    descricao_autuador,
    id_municipio_autuacao,
    descricao_municipio,
    uf_autuacao,
    cep_autuacao,
    tile_autuacao,
    processo_defesa_autuacao,
    recurso_penalidade_multa,
    processo_troca_real_infrator,
    status_sne,
    fonte
FROM
    citran