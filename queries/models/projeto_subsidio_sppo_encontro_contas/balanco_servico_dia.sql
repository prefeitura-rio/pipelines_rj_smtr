{% if var("encontro_contas_modo") == "" %}
-- 0. Lista servicos e dias atípicos (pagos por recurso)
WITH
  recursos AS (
  SELECT
    data,
    id_recurso,
    tipo_recurso,
    -- consorcio,
    servico,
    incorporado_algoritmo,
    SUM(valor_pago) AS valor_pago
  FROM
    {{ ref("recursos_sppo_servico_dia_pago") }}
    -- `rj-smtr`.`br_rj_riodejaneiro_recursos`.`recursos_sppo_servico_dia_pago`
  GROUP BY
    1,
    2,
    3,
    4,
    5),
servico_dia_atipico as (
SELECT
  DISTINCT data, servico
FROM
  recursos
WHERE
  -- Quando o valor do recurso pago for R$ 0, desconsidera-se o recurso, pois:
    -- Recurso pode ter sido cancelado (pago e depois revertido)
    -- Problema reporto não gerou impacto na operação (quando aparece apenas 1 vez)
  valor_pago != 0
  -- Desconsideram-se recursos do tipo "Algoritmo" (igual a apuração em produção, levantado pela TR/SUBTT/CMO)
  -- Desconsideram-se recursos do tipo "Viagem Individual" (não afeta serviço-dia)
  AND tipo_recurso NOT IN ("Algoritmo", "Viagem Individual", "Encontro de contas")
  -- Desconsideram-se recursos de reprocessamento que já constam em produção
  AND NOT (data BETWEEN "2022-06-01" AND "2022-06-30"
            AND tipo_recurso = "Reprocessamento")
  AND NOT incorporado_algoritmo
),

-- 1. Calcula a km subsidiada por servico e dia
sumario_dia AS (  -- Km apurada por servico e dia
  SELECT
    DATA,
    consorcio,
    servico,
    SUM(km_apurada) AS km_subsidiada,
    sum(valor_subsidio_pago) as subsidio_pago
  FROM
    {# {{ ref("sumario_servico_dia_historico") }} #}
    {# `rj-smtr.monitoramento.sumario_servico_dia_historico` #}
    {{ ref("staging_encontro_contas_sumario_servico_dia_historico") }}
  WHERE
    DATA BETWEEN "{{ var('start_date') }}"
    AND "{{ var('end_date') }}"
    and valor_subsidio_pago > 0
  GROUP BY
    1,
    2,
    3),
  viagem_remunerada AS ( -- Km subsidiada pos regra do teto de 120% por servico e dia
  SELECT
    DATA,
    servico,
    SUM(distancia_planejada) AS km_subsidiada
  FROM
    {# {{ ref("viagens_remuneradas") }} #}
    `rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas`
  WHERE
    DATA BETWEEN "{{ var('start_date') }}"
    AND "{{ var('end_date') }}"
    AND indicador_viagem_dentro_limite = TRUE -- useless
  GROUP BY
    1,
    2 ),
km_subsidiada_dia as (
  SELECT
  sd.* except(km_subsidiada),
    ifnull(case when data >= DATE('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}') then vr.km_subsidiada else sd.km_subsidiada end, 0) as km_subsidiada
  FROM
    sumario_dia sd
  LEFT JOIN
    viagem_remunerada as vr
  using
    (data, servico)
),

-- 2. Filtra km subsidiada apenas em dias típicos (remove servicos e dias pagos por recurso)
km_subsidiada_filtrada as (
  select
    ksd.*
  from km_subsidiada_dia ksd
  left join servico_dia_atipico sda
  using (data, servico)
  where sda.data is null
  -- Demais dias que não foi considerada a km apurada via GPS:
  and ksd.data NOT IN ("2022-10-02", "2022-10-30", '2023-02-07', '2023-02-08', '2023-02-10', '2023-02-13', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')
),


-- 3. Calcula a receita tarifaria por servico e dia
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
    round(SUM(receita_buc) + SUM(receita_buc_supervia) + SUM(receita_cartoes_perna_unica_e_demais) + SUM(receita_especie), 0) AS receita_tarifaria_aferida
  FROM
    {# {{ ref("rdo40_registros") }} #}
    `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo40_registros`
  WHERE
    DATA BETWEEN "{{ var('start_date') }}" AND "{{ var('end_date') }}"
    AND DATA NOT IN ("2022-10-02", "2022-10-30", '2023-02-07', '2023-02-08', '2023-02-10', '2023-02-13', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22')
    and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
  group by 1,2,3
),
-- 12. Lista os parâmetros de subsídio
parametros_raw AS (
SELECT
  data_inicio,
  data_fim,
  irk,
  subsidio_km,
  ( ( MAX(subsidio_km) OVER (PARTITION BY DATE_TRUNC(data_inicio, year),
        data_fim ) ) - subsidio_km ) AS desconto_subsidio_km
FROM
  `rj-smtr.subsidio.valor_km_tipo_viagem`
WHERE
  data_inicio >= ( DATE_TRUNC(DATE("{{ var('start_date') }}"), year) - INTERVAL 1 year - INTERVAL 10 day )
  AND data_fim <= ( DATE_TRUNC(DATE("{{ var('end_date') }}"), year) + INTERVAL 1 year + INTERVAL 10 day )
  AND subsidio_km > 0 ),
-- 13. Trata os parâmetros de subsídio
parametros_treated AS (
SELECT
  DISTINCT data_inicio,
  data_fim,
  irk,
  CASE
    WHEN data_fim <= "2022-12-31" THEN irk - subsidio_km -- subsidio varia ao longo dos meses
    ELSE irk - (subsidio_km + desconto_subsidio_km)
END
  AS irk_tarifa_publica,
  (subsidio_km + desconto_subsidio_km) AS subsidio_km,
  DATE_DIFF(data_fim, data_inicio, day) AS dias
FROM
  parametros_raw ),
-- 14. Lista os parâmetros de subsídio sem sobreposições temporais
parametros AS (
SELECT
  * EXCEPT (dias)
FROM
  parametros_treated AS pt
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY (SELECT MIN(p2.data_inicio)
    FROM
      parametros_treated p2
    WHERE
      p2.data_inicio < pt.data_fim
      AND p2.data_fim > pt.data_inicio )
  ORDER BY
    dias DESC ) = 1 -- Remove sobreposições temporais,  mantendo os maiores períodos
  )
  select
    *,
    ifnull(receita_total_aferida, 0) - ifnull(receita_total_esperada - subsidio_glosado, 0) as saldo
  from (
    select
      ks.* except(subsidio_pago),
      ks.km_subsidiada * par.irk as receita_total_esperada,
      ks.km_subsidiada * par.irk_tarifa_publica as receita_tarifaria_esperada,
      ks.km_subsidiada * par.subsidio_km as subsidio_esperado,
      case when data >= "2023-01-01" then (ks.km_subsidiada * par.subsidio_km - subsidio_pago) else 0 end as subsidio_glosado,
      ifnull(rdo.receita_tarifaria_aferida, 0) + ifnull(ks.subsidio_pago, 0) as receita_total_aferida,
      rdo.receita_tarifaria_aferida,
      ks.subsidio_pago
    from
      km_subsidiada_filtrada ks
    left join
      rdo
    using
      (data, servico, consorcio)
    left join
      parametros par
    on
      ks.data between data_inicio and data_fim
  )
{% else %}
{{ config(enabled=false) }}
{% endif %}