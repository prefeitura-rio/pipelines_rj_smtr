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
    {{ ref("licenciamento_stu_staging") }} AS t
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
  data,
  modo,
  id_veiculo,
  ano_fabricacao,
  carroceria,
  data_ultima_vistoria,
  id_carroceria,
  id_chassi,
  id_fabricante_chassi,
  id_interno_carroceria,
  id_planta,
  indicador_ar_condicionado,
  indicador_elevador,
  indicador_usb,
  indicador_wifi,
  nome_chassi,
  permissao,
  placa,
  quantidade_lotacao_pe,
  quantidade_lotacao_sentado,
  tipo_combustivel,
  tipo_veiculo,
  status,
  data_inicio_vinculo,
  ano_ultima_vistoria_atualizado,
  CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao,
  "{{ var("version") }}" AS versao
FROM
  stu_ano_ultima_vistoria
WHERE
  rn = 1

