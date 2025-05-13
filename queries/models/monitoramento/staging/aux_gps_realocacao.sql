{% if var("fifteen_minutes") == "_15_minutos" %} {{ config(materialized="ephemeral") }}
{% else %}
    {{
        config(
            materialized="incremental",
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
            alias=this.name ~ "_" ~ var("fonte_gps"),
        )
    }}
{% endif %}

-- 1. Filtra realocações válidas dentro do intervalo de GPS avaliado
with
    realocacao as (
        select
            * except (datetime_saida),
            case
                when datetime_saida is null then datetime_operacao else datetime_saida
            end as datetime_saida,
        from {{ ref("aux_realocacao") }}
        where
            -- Realocação deve acontecer após o registro de GPS e até 1 hora depois
            datetime_diff(datetime_operacao, datetime_entrada, minute) between 0 and 60
            and data between date("{{var('date_range_start')}}") and date(
                datetime_add("{{var('date_range_end')}}", interval 1 hour)
            )
            and (
                datetime_saida >= datetime("{{var('date_range_start')}}")
                or datetime_operacao >= datetime("{{var('date_range_start')}}")
            )
    ),
    -- 2. Altera registros de GPS com servicos realocados
    gps as (
        select id_veiculo, datetime_gps, servico, data
        from {{ ref("aux_gps") }}
        where
            data between date("{{var('date_range_start')}}") and date(
                "{{var('date_range_end')}}"
            )
            and datetime_gps > "{{var('date_range_start')}}"
            and datetime_gps <= "{{var('date_range_end')}}"
    ),
    combinacao as (
        select
            g.data,
            g.datetime_gps,
            r.id_veiculo,
            g.servico as servico_gps,
            r.servico as servico_realocado,
            r.datetime_operacao as datetime_realocado
        from gps g
        inner join
            realocacao r
            on g.id_veiculo = r.id_veiculo
            and g.servico != r.servico
            and g.datetime_gps between r.datetime_entrada and r.datetime_saida
    )
-- Filtra realocacao mais recente para cada timestamp
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_veiculo, datetime_gps order by datetime_realocado desc
            ) as rn
        from combinacao
    )
where rn = 1
