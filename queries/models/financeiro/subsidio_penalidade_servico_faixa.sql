{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    subsidio_dia as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}') and rn = 1
                then min_pof
                when data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                then pof
                else null
            end as min_pof
        from
            (
                select
                    *,
                    min(pof) over (
                        partition by data, tipo_dia, consorcio, servico
                    ) as min_pof,
                    row_number() over (
                        partition by data, tipo_dia, consorcio, servico
                        order by pof, faixa_horaria_inicio
                    ) as rn
                from {{ ref("subsidio_faixa_servico_dia") }}
                -- `rj-smtr.financeiro_staging.subsidio_faixa_servico_dia`
                {% if is_incremental() %}
                    where
                        data between date('{{ var("start_date") }}') and date(
                            '{{ var("end_date") }}'
                        )
                {% endif %}
            )
    ),
    penalidade as (
        select
            data_inicio,
            data_fim,
            perc_km_inferior,
            perc_km_superior,
            ifnull(- valor, 0) as valor_penalidade
        from {{ ref("valor_tipo_penalidade") }}
    -- `rj-smtr.dashboard_subsidio_sppo.valor_tipo_penalidade`
    )
select
    s.data,
    s.tipo_dia,
    s.consorcio,
    s.servico,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    safe_cast(coalesce(pe.valor_penalidade, 0) as numeric) as valor_penalidade,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from subsidio_dia as s
left join
    penalidade as pe
    on s.data between pe.data_inicio and pe.data_fim
    and s.min_pof >= pe.perc_km_inferior
    and s.min_pof < pe.perc_km_superior
