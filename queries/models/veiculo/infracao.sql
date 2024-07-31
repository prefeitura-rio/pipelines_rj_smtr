
{{
  config(
    materialized='incremental',
    partition_by={
      "field":"data",
      "data_type": "date",
      "granularity":"day"
    },
    unique_key=['data', 'id_auto_infracao'],
    incremental_strategy='insert_overwrite'
  )
}}

{%- if execute and is_incremental() %}
  {% set infracao_date = run_query("SELECT MIN(SAFE_CAST(data AS DATE)) FROM " ~ ref('infracao_staging') ~ " WHERE SAFE_CAST(data AS DATE) >= DATE_ADD(DATE('" ~ var("run_date") ~ "'), INTERVAL 7 DAY)").columns[0].values()[0] %}
{% endif -%}

WITH infracao AS (
  SELECT
    * EXCEPT(data),
    SAFE_CAST(data AS DATE) AS data
  FROM
    {{ ref("infracao_staging") }} as t
  {% if is_incremental() %}
    WHERE
      DATE(data) = DATE("{{ infracao_date }}")
  {% endif %}
),
infracao_rn AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY data, id_auto_infracao) rn
  FROM
    infracao
)
SELECT
  * EXCEPT(rn),
  CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao,
  "{{ var("version") }}" AS versao
FROM
  infracao_rn
WHERE
  rn = 1