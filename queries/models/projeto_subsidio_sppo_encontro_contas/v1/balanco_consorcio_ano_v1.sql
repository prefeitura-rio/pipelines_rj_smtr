{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %}
{{ config(enabled=false) }}
{% else %}
{{ config(alias=this.name ~ var('encontro_contas_modo')) }}
{% endif %}

select
  extract(year from data) as ano,
  consorcio,
  sum(km_subsidiada) as km_subsidiada,
  sum(receita_total_esperada) as receita_total_esperada,
  sum(receita_tarifaria_esperada) as receita_tarifaria_esperada,
  sum(subsidio_esperado) as subsidio_esperado,
  sum(subsidio_glosado) as subsidio_glosado,
  sum(receita_total_aferida) as receita_total_aferida,
  sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
  sum(subsidio_pago) as subsidio_pago,
  sum(saldo) as saldo
from {{ ref("balanco_servico_dia" ~ var('encontro_contas_modo'), v=1) }}
group by 1,2
order by 1,2