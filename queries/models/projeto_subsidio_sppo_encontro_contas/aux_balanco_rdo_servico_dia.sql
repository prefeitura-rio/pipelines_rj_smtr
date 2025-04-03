{{
    config(
        materialized="ephemeral",
    )
}}

WITH
-- 1. Calcula a receita tarifaria por servico e dia
rdo AS (
  SELECT
    data,
    consorcio,
    CASE
      WHEN LENGTH(linha) < 3 THEN LPAD(linha, 3, "0")
    ELSE
    CONCAT( IFNULL(REGEXP_EXTRACT(linha, r"[B-Z]+"), ""), IFNULL(REGEXP_EXTRACT(linha, r"[0-9]+"), "") )
  END
    AS servico,
    linha,
    tipo_servico,
    ordem_servico,
    round(SUM(receita_buc) + SUM(receita_buc_supervia) + SUM(receita_cartoes_perna_unica_e_demais) + SUM(receita_especie), 0) AS receita_tarifaria_aferida
  FROM
    {# {{ ref("rdo40_registros") }} #}
    `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo40_registros`
  WHERE
    DATA BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
    {# AND DATA NOT IN ("2022-10-02", "2022-10-30", '2023-02-07', '2023-02-08', '2023-02-10', '2023-02-13', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22') #}
    and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
    and (length(linha) != 4 and linha not like "2%") --  Remove rodoviarios
  group by 1,2,3,4,5,6
),
-- Remove servicos nao subsidiados
sumario_dia AS (
  SELECT
    DATA,
    consorcio,
    servico,
    SUM(km_apurada) AS km_subsidiada,
    sum(valor_subsidio_pago) as subsidio_pago
  FROM
    {# {{ ref("sumario_servico_dia_historico") }} #}
    `rj-smtr.monitoramento.sumario_servico_dia_historico`
  WHERE
    DATA BETWEEN "{{ var('start_date') }}"
    AND "{{ var('end_date') }}"
    and valor_subsidio_pago = 0
  GROUP BY
    1,
    2,
    3),
rdo_filtrada as (
    select rdo.* from rdo
    left join sumario_dia sd
    using (data, servico)
    where sd.servico is null
)
SELECT
  bsd.data,
  bsd.consorcio,
  bsd.servico,
  bsd.km_subsidiada,
  bsd.receita_tarifaria_aferida,
  rdo.data as data_rdo,
  rdo.consorcio as consorcio_rdo,
  rdo.servico as servico_tratado_rdo,
  rdo.linha as linha_rdo,
  rdo.tipo_servico as tipo_servico_rdo,
  rdo.ordem_servico as ordem_servico_rdo,
  rdo.receita_tarifaria_aferida as receita_tarifaria_aferida_rdo
FROM
  {{ ref("balanco_servico_dia" ~ var('encontro_contas_modo')) }} bsd
FULL JOIN
  rdo_filtrada rdo
USING
  (data, servico)
