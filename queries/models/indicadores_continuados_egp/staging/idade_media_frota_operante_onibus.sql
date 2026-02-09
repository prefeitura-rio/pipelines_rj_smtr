{{
  config(
    partition_by = {
    "field": "data",
    "data_type": "date",
    "granularity": "month"
    },
) }}

{% set incremental_filter %}
  {% if is_incremental() %}
    data BETWEEN DATE_TRUNC(DATE("{{ var('start_date') }}"), MONTH)
    AND LAST_DAY(DATE("{{ var('end_date') }}"), MONTH)
    AND data < DATE_TRUNC(CURRENT_DATE("America/Sao_Paulo"), MONTH)
    AND data >= DATE_TRUNC(DATE("{{ var('DATA_SUBSIDIO_V15_INICIO') }}"), MONTH)
  {% else %}
    data < DATE_TRUNC(CURRENT_DATE("America/Sao_Paulo"), MONTH)
  {% endif %}
{% endset %}

WITH
licenciamento AS (
  SELECT
    data,
    id_veiculo,
    ano_fabricacao
  FROM
    {{ ref("veiculo_dia") }}
  WHERE
    {{ incremental_filter }}
    AND data >= DATE_TRUNC(DATE("{{ var('DATA_SUBSIDIO_V15_INICIO') }}"), month)
  UNION DISTINCT
  SELECT
    data,
    id_veiculo,
    ano_fabricacao
  FROM
    {{ ref("sppo_licenciamento") }}
    --rj-smtr.veiculo.sppo_licenciamento
  WHERE
    {{ incremental_filter }}
    AND data < DATE_TRUNC(DATE("{{ var('DATA_SUBSIDIO_V15_INICIO') }}"), month)
),

-- 1. Seleciona a última data disponível de cada mês
datas AS (
  SELECT
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(YEAR FROM data) AS ano,
    MAX(data) AS data
  FROM
    licenciamento
  GROUP BY
    1,
    2
),

-- 2. Verifica frota operante
frota_operante AS (
  SELECT DISTINCT
    id_veiculo,
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(YEAR FROM data) AS ano
  FROM
    {{ ref('viagem_completa') }}
    --rj-smtr.projeto_subsidio_sppo.viagem_completa
  WHERE
    {{ incremental_filter }}
),

-- 3. Calcula a idade de todos os veículos para a data de referência
idade_frota AS (
  SELECT
    data,
    EXTRACT(YEAR FROM data) - CAST(ano_fabricacao AS INT64) AS idade
  FROM
    datas
  LEFT JOIN
    licenciamento
    --rj-smtr.veiculo.sppo_licenciamento AS l
    ON datas.data = licenciamento.data
LEFT JOIN
  frota_operante AS f
  USING
    (id_veiculo, mes, ano)
WHERE
  f.id_veiculo IS NOT NULL
)

-- 4. Calcula a idade média
SELECT
data,
EXTRACT(YEAR FROM data) AS ano,
EXTRACT(MONTH FROM data) AS mes,
"Ônibus" AS modo,
ROUND(AVG(idade), 2) AS idade_media_veiculo_mes,
CURRENT_DATE("America/Sao_Paulo") AS data_ultima_atualizacao,
'{{ var("version") }}' AS versao
FROM
idade_frota
GROUP BY
1,
2,
3
