with
    ipveuroiv_quantidade_geral as (
        select
            date_trunc(data, month) as data_inicial_mes,
            last_day(data, month) as data_final_mes,
            extract(year from data) as ano,
            extract(month from data) as mes,
            count(distinct id_veiculo) as denominador,
        from {{ ref("indicador_euro_vi") }}
        group by 1, 2, 3, 4
    ),
    ipveuroiv_quantidade_euro_iv as (
        select
            date_trunc(data, month) as data_inicial_mes,
            last_day(data, month) as data_final_mes,
            extract(year from data) as ano,
            extract(month from data) as mes,
            count(distinct id_veiculo) as numerador,
        from {{ ref("indicador_euro_vi") }} as iev
        where iev.indicador_euro_vi
        group by 1, 2, 3, 4
    ),
    indicador_euro_vi as (
        select
            data_inicial_mes,
            data_final_mes,
            ano,
            mes,
            "PVEUROIV" as indicador_codigo,
            "1.1" as indicador_versao,
            numerador as indicador_numerador,
            denominador as indicador_denominador,
            safe_divide(numerador, denominador) * 100 as indicador_valor,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from ipveuroiv_quantidade_geral
        full join
            ipveuroiv_quantidade_euro_iv using (
                data_inicial_mes, data_final_mes, ano, mes
            )
    )
select *
from indicador_euro_vi
