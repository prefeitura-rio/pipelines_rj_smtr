{{
    config(
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2024, "end": 2100, "interval": 1},
        },
    )
}}

select
    extract(year from data) as ano,
    servico,
    consorcio,
    sum(km_apurada) as km_apurada,
    sum(receita_total_esperada) as receita_total_esperada,
    sum(receita_tarifaria_esperada) as receita_tarifaria_esperada,
    sum(subsidio_esperado) as subsidio_esperado,
    sum(subsidio_glosado) as subsidio_glosado,
    sum(receita_total_aferida) as receita_total_aferida,
    sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
    sum(valor_subsidio_pago) as subsidio_pago,
    sum(saldo) as saldo,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("balanco_servico_dia") }}
group by all
