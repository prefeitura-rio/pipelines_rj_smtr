{{ config(materialized="ephemeral") }}

with
    registros as (
        select
            id_veiculo,
            servico,
            latitude,
            longitude,
            data,
            posicao_veiculo_geo,
            datetime_gps
        from {{ ref("aux_gps_filtrada") }} r
        {% if not flags.FULL_REFRESH -%}
            where
                data between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and datetime_gps > "{{var('date_range_start')}}"
                and datetime_gps <= "{{var('date_range_end')}}"
        {%- endif -%}
    ),
    intersec as (
        select
            r.*,
            s.feed_version,
            s.servico,
            s.route_id,
            -- 1. Histórico de intersecções nos últimos 10 minutos a partir da
            -- datetime_gps atual
            case
                when
                    count(
                        case
                            when
                                st_dwithin(
                                    shape,
                                    posicao_veiculo_geo,
                                    {{ var("tamanho_buffer_metros") }}
                                )
                            then 1
                        end
                    ) over (
                        partition by id_veiculo
                        order by
                            unix_seconds(timestamp(datetime_gps))
                            range
                            between {{ var("intervalo_max_desvio_segundos") }} preceding
                            and current row
                    )
                    >= 1
                then true
                else false
            end as indicador_trajeto_correto,
        -- 2. Join com data_versao_efetiva para definição de quais shapes serão
        -- considerados no cálculo do indicador
        from registros r
        left join
            (
                select *
                from {{ ref("viagem_planejada_planejamento") }}
                left join
                    {{ ref("shapes_geom_gtfs") }} using (
                        feed_version, feed_start_date, shape_id
                    )
                where
                    data between date("{{var('date_range_start')}}") and date(
                        "{{var('date_range_end')}}"
                    )
            ) s
            on r.servico = s.servico
            and r.data = s.data
    )
-- 3. Agregação com LOGICAL_OR para evitar duplicação de registros
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    route_id,
    logical_or(indicador_trajeto_correto) as indicador_trajeto_correto
from intersec i
group by id_veiculo, servico, route_id, data, datetime_gps
