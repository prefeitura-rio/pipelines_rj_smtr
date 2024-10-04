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
    SELECT
        data,
        id_auto_infracao,
        DATETIME(concat(data,' ',hora,':00')) AS datetime_autuacao,
        data_limite_defesa_previa,
        data_limite_recurso,
        situacao_atual AS descricao_situacao_autuacao,
        IF(status_infracao != "", status_infracao, NULL) AS status_infracao,
        SUBSTR(REPLACE(codigo_enquadramento, '-', ''), 1, 4) AS codigo_enquadramento,
        IF(tipificacao_resumida != "", tipificacao_resumida, NULL) AS tipificacao_resumida,
        SAFE_CAST(SUBSTR(REGEXP_EXTRACT(pontuacao, r'\d+'), 2) AS STRING) AS pontuacao,
        CASE
            WHEN INITCAP(REGEXP_REPLACE(pontuacao, r'\d+', '')) = 'Media' THEN 'Média'
            WHEN INITCAP(REGEXP_REPLACE(pontuacao, r'\d+', '')) = 'Gravissima' THEN 'Gravíssima'
            ELSE INITCAP(REGEXP_REPLACE(pontuacao, r'\d+', ''))
        END AS gravidade,
        SAFE_CAST(NULL AS STRING) AS amparo_legal,
        INITCAP(tipo_veiculo) AS tipo_veiculo,
        IF(descricao_veiculo != "", descricao_veiculo, NULL) AS descricao_veiculo,
        SAFE_CAST(NULL AS STRING) AS placa_veiculo,
        SAFE_CAST(NULL AS STRING) AS ano_fabricacao_veiculo,
        SAFE_CAST(NULL AS STRING) AS ano_modelo_veiculo,
        SAFE_CAST(NULL AS STRING) AS cor_veiculo,
        CASE
            WHEN INITCAP(REGEXP_REPLACE(especie_veiculo, r'\d+', '')) IN ('Misto', '0Misto') THEN 'Misto'
            WHEN INITCAP(REGEXP_REPLACE(especie_veiculo, r'\d+', '')) IN ('Passageir', '0Passageir', 'Passageiro', '0Passageiro') THEN 'Passageiro'
            WHEN INITCAP(REGEXP_REPLACE(especie_veiculo, r'\d+', '')) IN ('Tracao', '0Tracao', 'Tracao') THEN 'Tração'
            WHEN INITCAP(REGEXP_REPLACE(especie_veiculo, r'\d+', '')) IN ('Nao Inform', '0Nao Inform', 'Nao Informado', '0Nao Informado') THEN 'Não informado'
            WHEN INITCAP(REGEXP_REPLACE(especie_veiculo, r'\d+', '')) IN ('Carga', '0Carga') THEN 'Carga'
            ELSE 'Inválido'
        END AS especie_veiculo,
        SAFE_CAST(NULL AS STRING) AS uf_infrator,
        SAFE_CAST(NULL AS STRING) AS uf_principal_condutor,
        IF(uf_proprietario != "", uf_proprietario, NULL) AS uf_proprietario,
        IF(cep_proprietario != "", cep_proprietario, NULL) AS cep_proprietario,
        valor_infracao / 100 AS valor_infracao,
        valor_pago  / 100 AS valor_pago,
        data_pagamento,
        "260010" AS id_autuador,
        IF(descricao_autuador != "", descricao_autuador, NULL) AS descricao_autuador,
        "6001" AS id_municipio_autuacao,
        "RIO DE JANEIRO" AS descricao_municipio,
        "RJ" AS uf_autuacao,
        NULL AS cep_autuacao, -- não padronizado na citran
        NULL AS tile_autuacao,
        IF(processo_defesa_autuacao != "00000000" AND processo_defesa_autuacao != "" , processo_defesa_autuacao, NULL) AS processo_defesa_autuacao,
        IF(recurso_penalidade_multa != "00000000" AND recurso_penalidade_multa != "" , recurso_penalidade_multa, NULL) AS recurso_penalidade_multa,
        IF(processo_troca_real_infrator != "00000000" AND processo_troca_real_infrator != "" , processo_troca_real_infrator, NULL) AS processo_troca_real_infrator,
        FALSE AS status_sne,
        "CITRAN" AS fonte
    FROM {{ ref('autuacao_citran') }}
    {% if is_incremental() %}
        WHERE
            data BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    {% endif %}
),
serpro AS (
    SELECT
        data,
        id_auto_infracao,
        datetime_autuacao,
        data_limite_defesa_previa,
        data_limite_recurso,
        descricao_situacao_autuacao,
        IF(status_infracao != "", status_infracao, NULL) AS status_infracao,
        IF(codigo_enquadramento != "", codigo_enquadramento, NULL) AS codigo_enquadramento,
        IF(tipificacao_resumida != "", tipificacao_resumida, NULL) AS tipificacao_resumida,
        SUBSTR(pontuacao, 1, 1) AS pontuacao,
        gravidade,
        amparo_legal,
        INITCAP(tipo_veiculo) AS tipo_veiculo,
        IF(descricao_veiculo != "", descricao_veiculo, NULL) AS descricao_veiculo,
        placa_veiculo,
        ano_fabricacao_veiculo,
        ano_modelo_veiculo,
        cor_veiculo,
        especie_veiculo,
        uf_infrator,
        uf_principal_condutor,
        IF(uf_proprietario != "", uf_proprietario, NULL) AS uf_proprietario,
        SAFE_CAST(NULL AS STRING) AS cep_proprietario,
        valor_infracao,
        valor_pago,
        SAFE_CAST(NULL AS STRING) AS data_pagamento,
        COALESCE(id_autuador, "260010") AS id_autuador,
        IF(descricao_autuador != "", descricao_autuador, NULL) AS descricao_autuador,
        COALESCE(id_municipio_autuacao,"6001") AS id_municipio_autuacao,
        COALESCE(descricao_municipio, "RIO DE JANEIRO") AS descricao_municipio,
        COALESCE(uf_autuacao,"RJ") AS uf_autuacao,
        NULL AS cep_autuacao,
        NULL AS tile_autuacao,
        processo_defesa_autuacao,
        recurso_penalidade_multa,
        processo_troca_real_infrator,
        IF(status_sne = "1.0", TRUE, FALSE) AS status_sne,
        "SERPRO" AS fonte
    FROM {{ ref('autuacao_serpro') }}
    {% if is_incremental() %}
        WHERE
            data BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
    {% endif %}
)
SELECT
    data,
    TO_HEX(SHA256(CONCAT(GENERATE_UUID(), id_auto_infracao))) AS id_autuacao,
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
UNION ALL
SELECT
    data,
    TO_HEX(SHA256(CONCAT(GENERATE_UUID(), id_auto_infracao))) AS id_autuacao,
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
    serpro