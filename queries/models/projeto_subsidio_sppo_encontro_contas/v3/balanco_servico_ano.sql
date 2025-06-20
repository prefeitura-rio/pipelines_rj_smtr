select
    extract(year from data) as ano,
    consorcio,
    servico,
    sum(km_apurada) as km_apurada,
    sum(receita_tarifaria_esperada) as receita_tarifaria_esperada,
    sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
    sum(saldo) as saldo
from {{ ref("balanco_servico_dia") }}
group by all
order by 1, 2, 3
