{{ config(materialized="table") }}

WITH
tratado AS (
  SELECT
    c.modo,
    c.id_consorcio,
    c.consorcio,
    o.id_operadora,
    o.operadora,
    lco.cd_linha AS id_servico_jae,
    l.nr_linha AS servico_jae,
    l.nm_linha AS descricao_servico_jae,
    lt.tarifa_ida,
    lt.tarifa_volta,
    coalesce(l.gtfs_route_id, l.gtfs_stop_id) AS id_servico_gtfs,
    CASE
      WHEN l.gtfs_route_id IS NOT null
        THEN 'routes'
      WHEN l.gtfs_stop_id IS NOT null
        THEN 'stops'
    END AS tabela_origem_gtfs,
    CASE
      WHEN datetime(lco.dt_inicio_validade) > lt.dt_inicio_validade
        THEN datetime(lco.dt_inicio_validade)
      ELSE lt.dt_inicio_validade
    END AS data_inicio_validade,
    CASE
      WHEN lco.dt_fim_validade IS null AND lt.data_fim_validade IS NOT null
        THEN lt.data_fim_validade
      WHEN lco.dt_fim_validade IS NOT null AND lt.data_fim_validade IS null
        THEN lco.dt_fim_validade
      WHEN datetime(lco.dt_fim_validade) > lt.data_fim_validade
        THEN lt.data_fim_validade
      WHEN datetime(lco.dt_fim_validade) < lt.data_fim_validade
        THEN datetime(lco.dt_fim_validade)
    END AS data_fim_validade
  FROM {{ ref("staging_linha_consorcio_operadora_transporte") }} AS lco
  INNER JOIN
    {{ ref("operadoras") }} AS o
    ON lco.cd_operadora_transporte = o.id_operadora_jae
  INNER JOIN
    {{ ref("consorcios") }} AS c
    ON lco.cd_consorcio = c.id_consorcio_jae
  INNER JOIN {{ ref("staging_linha") }} AS l ON lco.cd_linha = l.cd_linha
  LEFT JOIN {{ ref("aux_linha_tarifa") }} AS lt ON lco.cd_linha = lt.cd_linha
  WHERE
    (
      (
        lt.data_fim_validade IS NOT null
        AND datetime(lco.dt_inicio_validade) < lt.data_fim_validade
      )
      AND (
        lco.dt_fim_validade IS NOT null
        AND datetime(lt.dt_inicio_validade) < lco.dt_fim_validade
      )
    )
    OR (lt.data_fim_validade IS null OR lco.dt_fim_validade IS null)
)

SELECT
  *,
  '{{ var("version") }}' AS versao
FROM tratado
WHERE data_inicio_validade < data_fim_validade OR data_fim_validade IS null
