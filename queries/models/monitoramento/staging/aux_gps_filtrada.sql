{{ config(materialized="ephemeral") }}

with
    box as (
        /* Geometria de caixa que contém a área do município de Rio de Janeiro.*/
        select * from {{ var("limites_caixa") }}
    ),
    gps as (
        select *, st_geogpoint(longitude, latitude) posicao_veiculo_geo
        from {{ ref("aux_gps") }}
        where
            data between date("{{var('date_range_start')}}") and date(
                "{{var('date_range_end')}}"
            )
            {% if is_incremental() -%}
                and datetime_gps > "{{var('date_range_start')}}"
                and datetime_gps <= "{{var('date_range_end')}}"
            {%- endif -%}
    ),
    realocacao as (
        select g.* except (servico), coalesce(r.servico_realocado, g.servico) as servico
        from gps g
        left join {{ ref("aux_gps_realocacao") }} r using (id_veiculo, datetime_gps)
    ),
    filtrada as (
        select
            data,
            hora,
            datetime_gps,
            id_veiculo,
            servico,
            latitude,
            longitude,
            posicao_veiculo_geo,
            datetime_captura,
            velocidade,
            row_number() over (partition by id_veiculo, datetime_gps, servico) rn
        from realocacao
        where
            st_intersectsbox(
                posicao_veiculo_geo,
                (select min_longitude from box),
                (select min_latitude from box),
                (select max_longitude from box),
                (select max_latitude from box)
            )
    )
select * except (rn), "{{ var('version') }}" as versao
from filtrada
where rn = 1
