-- fmt: off
{{
    config(
        materialized = 'ephemeral' if var('15_minutos', false) else 'incremental',
        partition_by = {"field": "data", "data_type": "date", "granularity": "day"} if not var('15_minutos', false) else none,
    )
}}
-- fmt: on

-- 1. Filtra realocações válidas dentro do intervalo de GPS avaliado
with
    realocacao as (
        select
            * except (datetime_saida),
            case
                when datetime_saida is null then datetime_operacao else datetime_saida
            end as datetime_saida,
        from {{ ref("sppo_realocacao") }}
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
        select ordem, timestamp_gps, linha, data, hora
        from {{ ref("sppo_registros") }}
        where
            data between date("{{var('date_range_start')}}") and date(
                "{{var('date_range_end')}}"
            )
            and timestamp_gps > "{{var('date_range_start')}}"
            and timestamp_gps <= "{{var('date_range_end')}}"
    ),
    combinacao as (
        select
            r.id_veiculo,
            g.timestamp_gps,
            g.linha as servico_gps,
            r.servico as servico_realocado,
            r.datetime_operacao as datetime_realocacao,
            g.data,
            g.hora
        from gps g
        inner join
            realocacao r
            on g.ordem = r.id_veiculo
            and g.linha != r.servico
            and g.timestamp_gps between r.datetime_entrada and r.datetime_saida
    )
-- Filtra realocacao mais recente para cada timestamp
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_veiculo, timestamp_gps order by datetime_realocacao desc
            ) as rn
        from combinacao
    )
where rn = 1
