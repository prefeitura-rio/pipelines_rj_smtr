-- depends_on: {{ ref('aux_sppo_licenciamento_vistoria_atualizada') }}
{{
  config(
    materialized="incremental",
    partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    unique_key=["data", "id_veiculo"],
    incremental_strategy="insert_overwrite",
  )
}}

{% if execute and is_incremental() %}
  {% set licenciamento_date = run_query(get_license_date()).columns[0].values()[0] %}
{% endif %}

WITH stu AS (
  SELECT
    * EXCEPT(data),
    DATE(data) AS data
  FROM
    {{ ref("sppo_licenciamento_stu_staging") }} AS t
  {% if is_incremental() %}
    WHERE
      DATE(data) = DATE("{{ licenciamento_date }}")
  {% endif %}
),
stu_rn AS (
  SELECT
    * EXCEPT (timestamp_captura),
    EXTRACT(YEAR FROM data_ultima_vistoria) AS ano_ultima_vistoria,
    ROW_NUMBER() OVER (PARTITION BY data, id_veiculo) rn
  FROM
    stu
),
stu_ano_ultima_vistoria AS (
  -- Temporariamente considerando os dados de vistoria enviados pela TR/SUBTT/CGLF
  SELECT
    s.* EXCEPT(ano_ultima_vistoria),
    CASE
      WHEN data >= "2024-03-01" AND c.ano_ultima_vistoria > s.ano_ultima_vistoria THEN c.ano_ultima_vistoria
      WHEN data >= "2024-03-01" THEN COALESCE(s.ano_ultima_vistoria, c.ano_ultima_vistoria)
      ELSE s.ano_ultima_vistoria
    END AS ano_ultima_vistoria_atualizado,
  FROM
    stu_rn AS s
  LEFT JOIN
  (
    SELECT
      id_veiculo,
      placa,
      ano_ultima_vistoria
    FROM
      {{ ref("aux_sppo_licenciamento_vistoria_atualizada") }}
  ) AS c
  USING(id_veiculo, placa)
)
SELECT
  * EXCEPT(rn),
FROM
  stu_ano_ultima_vistoria
WHERE
  rn = 1

