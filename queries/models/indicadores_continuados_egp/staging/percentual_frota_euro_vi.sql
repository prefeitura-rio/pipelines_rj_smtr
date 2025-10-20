{{
    config(
        partition_by={"field": "data", "data_type": "date", "granularity": "month"},
    )
}}
with
    veiculos as (
        select
            DATE_TRUNC(data, MONTH) AS data,
            extract(year from data) as ano,
            extract(month from data) as mes,
            id_veiculo,
            ano_fabricacao
        from {{ ref("veiculo_dia") }}
        where
            {% if is_incremental() %}
                data between date_trunc(
                    date("{{ var(" start_date ") }}"), month
                ) and last_day(date("{{ var(" end_date ") }}"), month)
                and data < date_trunc(current_date("America/Sao_Paulo"), month)
            {% else %} data < date_trunc(current_date("America/Sao_Paulo"), month)
            {% endif %}
            and tipo_veiculo not in (
                '44 BRT PADRON',
                '45 BRT ARTICULADO',
                '46 BRT BIARTICULADO',
                '61 RODOV. C/AR E ELEV',
                '5 ONIBUS ROD. C/ AR'
            )
            and ano_fabricacao is not null
    ),
    veiculos_por_mes as (
        select data, ano, mes, count(distinct id_veiculo) as total_veiculos_mes
        from veiculos
        group by 1, 2
    ),
    veiculos_euro_vi as (
        select data, ano, mes, count(distinct id_veiculo) as veiculos_euro_vi
        from veiculos
        where ano_fabricacao >= 2023
        group by 1, 2
    )

select
    v.data,
    v.ano,
    v.mes,
    "Ônibus" as modo,
    v.total_veiculos_mes,
    coalesce(v2.veiculos_euro_vi, 0) as veiculos_euro_vi,
    safe_divide(v2.veiculos_euro_vi, v.total_veiculos_mes)
    * 100 as percentual_veiculos_euro_vi,
    current_date("America/Sao_Paulo") as data_ultima_atualizacao,
    '{{ var("version") }}' as versao
from veiculos_por_mes v
left join veiculos_euro_vi v2 using (data, ano, mes)
order by v.ano, v.mes

