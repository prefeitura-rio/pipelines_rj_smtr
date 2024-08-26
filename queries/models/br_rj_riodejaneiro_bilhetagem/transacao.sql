-- depends_on: {{ ref('operadoras_contato') }}
-- depends_on: {{ ref('transacao') }}
{{
  config(
    materialized="incremental",
    partition_by={
      "field":"data",
      "data_type":"date",
      "granularity": "day"
    },
    incremental_strategy="insert_overwrite"
  )
}}

-- TODO: Usar variável de run_date_hour para otimizar o numero de partições lidas em staging
{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
{% endset %}

{% set transacao_staging = ref('staging_transacao') %}
{% set integracao_staging = ref('staging_integracao_transacao') %}
{% if execute %}
  {% if is_incremental() %}
    {% set transacao_partitions_query %}
      WITH particoes_integracao AS (
        SELECT DISTINCT
          CONCAT("'", DATE(data_transacao), "'") AS data_transacao
        FROM
          {{ integracao_staging }},
          UNNEST([
            data_transacao_t0,
            data_transacao_t1,
            data_transacao_t2,
            data_transacao_t3,
            data_transacao_t4
          ]) AS data_transacao
        WHERE
          {{ incremental_filter }}
        ),
        particoes_transacao AS (
          SELECT DISTINCT
            CONCAT("'", DATE(data_transacao), "'") AS data_transacao
          FROM
            {{ transacao_staging }}
          WHERE
            {{ incremental_filter }}
        )
        SELECT
          COALESCE(t.data_transacao, i.data_transacao) AS data_transacao
        FROM
          particoes_transacao t
        FULL OUTER JOIN
          particoes_integracao i
        USING(data_transacao)
        WHERE
          COALESCE(t.data_transacao, i.data_transacao) IS NOT NULL
    {% endset %}

    {% set transacao_partitions = run_query(transacao_partitions_query) %}

    {% set transacao_partition_list = transacao_partitions.columns[0].values() %}
  {% endif %}
{% endif %}

WITH transacao AS (
  SELECT
    *
  FROM
    {{ transacao_staging }}
  {% if is_incremental() %}
      WHERE
        {{ incremental_filter }}
  {% endif %}
),
tipo_transacao AS (
  SELECT
    chave AS id_tipo_transacao,
    valor AS tipo_transacao,
  FROM
    `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
  WHERE
    id_tabela = "transacao"
    AND coluna = "id_tipo_transacao"
),
gratuidade AS (
  SELECT
    CAST(id_cliente AS STRING) AS id_cliente,
    tipo_gratuidade,
    data_inicio_validade,
    data_fim_validade
  FROM
    {{ ref("gratuidade_aux") }}
  -- TODO: FILTRAR PARTIÇÕES DE FORMA EFICIENTE
),
tipo_pagamento AS (
  SELECT
    chave AS id_tipo_pagamento,
    valor AS tipo_pagamento
  FROM
    `rj-smtr.br_rj_riodejaneiro_bilhetagem.dicionario`
  WHERE
    id_tabela = "transacao"
    AND coluna = "id_tipo_pagamento"
),
integracao AS (
  SELECT
    id_transacao,
    valor_rateio,
    datetime_processamento_integracao
  FROM
    {{ ref("integracao") }}
  {% if is_incremental() %}
    WHERE
    {% if transacao_partition_list|length > 0 %}
      data IN ({{ transacao_partition_list|join(', ') }})
    {% else %}
      data = "2000-01-01"
    {% endif %}
  {% endif %}
),
new_data AS (
  SELECT
    EXTRACT(DATE FROM data_transacao) AS data,
    EXTRACT(HOUR FROM data_transacao) AS hora,
    data_transacao AS datetime_transacao,
    data_processamento AS datetime_processamento,
    t.timestamp_captura AS datetime_captura,
    m.modo,
    dc.id_consorcio,
    dc.consorcio,
    do.id_operadora,
    do.operadora,
    t.cd_linha AS id_servico_jae,
    -- s.servico,
    l.nr_linha AS servico_jae,
    l.nm_linha AS descricao_servico_jae,
    sentido,
    CASE
      WHEN m.modo = "VLT" THEN SUBSTRING(t.veiculo_id, 1, 3)
      WHEN m.modo = "BRT" THEN NULL
      ELSE t.veiculo_id
    END AS id_veiculo,
    t.numero_serie_validador AS id_validador,
    COALESCE(t.id_cliente, t.pan_hash) AS id_cliente,
    id AS id_transacao,
    tp.tipo_pagamento,
    tt.tipo_transacao,
    g.tipo_gratuidade,
    tipo_integracao AS id_tipo_integracao,
    NULL AS id_integracao,
    latitude_trx AS latitude,
    longitude_trx AS longitude,
    ST_GEOGPOINT(longitude_trx, latitude_trx) AS geo_point_transacao,
    NULL AS stop_id,
    NULL AS stop_lat,
    NULL AS stop_lon,
    valor_transacao
  FROM
    transacao AS t
  LEFT JOIN
    {{ source("cadastro", "modos") }} m
  ON
    t.id_tipo_modal = m.id_modo AND m.fonte = "jae"
  LEFT JOIN
    {{ ref("operadoras") }} do
  ON
    t.cd_operadora = do.id_operadora_jae
  LEFT JOIN
    {{ ref("consorcios") }} dc
  ON
    t.cd_consorcio = dc.id_consorcio_jae
  LEFT JOIN
    {{ ref("staging_linha") }} l
  ON
    t.cd_linha = l.cd_linha
  -- LEFT JOIN
  --     {{ ref("servicos") }} AS s
  -- ON
  --     t.cd_linha = s.id_servico_jae
  LEFT JOIN
    tipo_transacao tt
  ON
    tt.id_tipo_transacao = t.tipo_transacao
  LEFT JOIN
    tipo_pagamento tp
  ON
    t.id_tipo_midia = tp.id_tipo_pagamento
  LEFT JOIN
    gratuidade g
  ON
    t.tipo_transacao = "21"
    AND t.id_cliente = g.id_cliente
    AND t.data_transacao >= g.data_inicio_validade
    AND (t.data_transacao < g.data_fim_validade OR g.data_fim_validade IS NULL)
  LEFT JOIN
    {{ ref("staging_linha_sem_ressarcimento") }} lsr
  ON
    t.cd_linha = lsr.id_linha
  WHERE
    lsr.id_linha IS NULL
    AND DATE(data_transacao) >= "2023-07-17"
),
complete_partitions AS (
  SELECT
    data,
    hora,
    datetime_transacao,
    datetime_processamento,
    datetime_captura,
    modo,
    id_consorcio,
    consorcio,
    id_operadora,
    operadora,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    sentido,
    id_veiculo,
    id_validador,
    id_cliente,
    id_transacao,
    tipo_pagamento,
    tipo_transacao,
    tipo_gratuidade,
    id_tipo_integracao,
    id_integracao,
    latitude,
    longitude,
    geo_point_transacao,
    stop_id,
    stop_lat,
    stop_lon,
    valor_transacao,
    0 AS priority
  FROM
    new_data

  {% if is_incremental() %}
    UNION ALL

    SELECT
      data,
      hora,
      datetime_transacao,
      datetime_processamento,
      datetime_captura,
      modo,
      id_consorcio,
      consorcio,
      id_operadora,
      operadora,
      id_servico_jae,
      servico_jae,
      descricao_servico_jae,
      sentido,
      id_veiculo,
      id_validador,
      id_cliente,
      id_transacao,
      tipo_pagamento,
      tipo_transacao,
      tipo_gratuidade,
      id_tipo_integracao,
      id_integracao,
      latitude,
      longitude,
      geo_point_transacao,
      stop_id,
      stop_lat,
      stop_lon,
      valor_transacao,
      1 AS priority
    FROM
      {{ this }}
    WHERE
      {% if transacao_partition_list|length > 0 %}
        data IN ({{ transacao_partition_list|join(', ') }})
      {% else %}
        data = "2000-01-01"
      {% endif %}
  {% endif %}
),
transacao_deduplicada AS (
  SELECT
    * EXCEPT(rn)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id_transacao ORDER BY datetime_captura DESC, priority) AS rn
    FROM
      complete_partitions
  )
  WHERE
    rn = 1
)
SELECT
  t.data,
  t.hora,
  t.datetime_transacao,
  t.datetime_processamento,
  t.datetime_captura,
  t.modo,
  t.id_consorcio,
  t.consorcio,
  t.id_operadora,
  t.operadora,
  t.id_servico_jae,
  t.servico_jae,
  t.descricao_servico_jae,
  t.sentido,
  t.id_veiculo,
  t.id_validador,
  t.id_cliente,
  t.id_transacao,
  t.tipo_pagamento,
  t.tipo_transacao,
  CASE
    WHEN t.tipo_transacao = "Integração" OR i.id_transacao IS NOT NULL THEN "Integração"
    WHEN t.tipo_transacao IN ("Débito", "Botoeira") THEN "Integral"
    ELSE t.tipo_transacao
  END AS tipo_transacao_smtr,
  CASE
    WHEN t.tipo_transacao = "Gratuidade" AND t.tipo_gratuidade IS NULL THEN "Não Identificado"
    ELSE t.tipo_gratuidade
  END AS tipo_gratuidade,
  t.id_tipo_integracao,
  t.id_integracao,
  t.latitude,
  t.longitude,
  t.geo_point_transacao,
  t.stop_id,
  t.stop_lat,
  t.stop_lon,
  t.valor_transacao,
  CASE
    WHEN
      i.id_transacao IS NOT NULL
      OR DATE(t.datetime_processamento) < (SELECT MAX(data_ordem) FROM {{ ref("ordem_pagamento_dia") }})
      THEN COALESCE(i.valor_rateio, t.valor_transacao) * 0.96
  END AS valor_pagamento,
  '{{ var("version") }}' AS versao
FROM
  transacao_deduplicada t
LEFT JOIN
  integracao i
USING(id_transacao)