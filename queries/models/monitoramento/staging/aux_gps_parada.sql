{{ config(materialized="ephemeral") }}

{% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
{% endset %}

{# {% set calendario = ref("calendario") %} #}
{% set calendario = "rj-smtr.planejamento.calendario" %}
{% if execute %}
    {% set gtfs_feeds_query %}
        select distinct concat("'", feed_start_date, "'") as feed_start_date
        from {{ calendario }}
        where {{ incremental_filter }}
    {% endset %}
    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
{% endif %}

with
    terminais as (
        -- 1. Selecionamos terminais, criando uma geometria de ponto para cada.
        select
            st_geogpoint(stop_lon, stop_lat) as ponto_parada,
            stop_name as nome_parada,
            'terminal' as tipo_parada
        -- from {{ ref("stops_gtfs") }}
        from `rj-smtr`.`gtfs`.`stops`
        where location_type = "1" and feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    garagens as (
        -- 1. Selecionamos as garagens, , criando uma geometria de ponto para cada.
        select distinct
            st_astext(st_geogpoint(stop_lon, stop_lat)) as ponto_parada,
            stop_name as nome_parada,
            'garagens' as tipo_parada
        -- from {{ ref("stops_gtfs") }}
        from `rj-smtr`.`gtfs`.`stops`
        left join
            -- {{ ref("stop_times_gtfs") }} using (feed_version, feed_start_date,
            -- stop_id)
            `rj-smtr`.`gtfs`.`stop_times` using (feed_version, feed_start_date, stop_id)
        where
            pickup_type is null
            and drop_off_type is null
            and stop_name like "%Garagem%"
            and feed_start_date in ({{ gtfs_feeds | join(", ") }})
    ),
    pontos_parada as (
        -- Unimos terminais e garagens para obter todos os pontos de parada
        select *
        from terminais
        union all
        select st_geogfromtext(ponto_parada) as ponto_parada, * except (ponto_parada)
        from garagens
    ),
    distancia as (
        -- 2. Calculamos as distâncias e definimos nrow
        select
            id_veiculo,
            datetime_gps,
            data,
            servico,
            posicao_veiculo_geo,
            nome_parada,
            tipo_parada,
            round(st_distance(posicao_veiculo_geo, ponto_parada), 1) distancia_parada,
            row_number() over (
                partition by datetime_gps, id_veiculo, servico
                order by st_distance(posicao_veiculo_geo, ponto_parada)
            ) nrow
        from pontos_parada p
        join
            (
                select id_veiculo, datetime_gps, data, servico, posicao_veiculo_geo
                from {{ ref("aux_gps_filtrada") }}
                {% if not flags.FULL_REFRESH %}
                    where
                        {{ incremental_filter }}
                        and datetime_gps > "{{var('date_range_start')}}"
                        and datetime_gps <= "{{var('date_range_end')}}"
                {% endif %}
            ) r
            on 1 = 1
    )
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    /*
  3. e 4. Identificamos o status do veículo como 'terminal', 'garagem' (para os veículos parados) ou
  null (para os veículos mais distantes de uma parada que o limiar definido)
  */
    case
        when distancia_parada < {{ var("distancia_limiar_parada") }}
        then tipo_parada
        else null
    end tipo_parada,
from distancia
where nrow = 1
