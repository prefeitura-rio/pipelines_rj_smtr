{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        tags=["geolocalizacao"],
    )
}}

-- depends_on: {{ ref('feed_info_gtfs') }}
{% if execute and is_incremental() %}
    {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
{% endif %}

with
    aux_segmento as (
        select
            feed_version,
            feed_start_date,
            feed_end_date,
            shape_id,
            id_segmento,
            st_geogfromtext(wkt_segmento) as segmento,
            wkt_segmento,
            round(st_length(st_geogfromtext(wkt_segmento)), 1) as comprimento_segmento
        from {{ ref("aux_segmento_shape") }}
    ),
    tunel as (
        select st_union_agg(st_buffer(geometry, 50)) as buffer_tunel
        from {{ source("dados_mestres", "logradouro") }}
        where tipo = "Túnel"

    ),
    buffer_segmento as (
        select *, st_buffer(segmento, 20) as buffer_completo, from aux_segmento
    ),
    intercessao_segmento as (
        select
            b1.shape_id,
            b1.id_segmento,
            st_union(array_agg(b2.buffer_completo)) as buffer_segmento_posterior
        from buffer_segmento b1
        join
            buffer_segmento b2
            on b1.shape_id = b2.shape_id
            and b1.id_segmento < b2.id_segmento
            and st_intersects(b1.buffer_completo, b2.buffer_completo)
        group by 1, 2
    ),
    buffer_segmento_recortado as (
        select
            b.*,
            coalesce(
                st_difference(buffer_completo, i.buffer_segmento_posterior),
                buffer_completo
            ) as buffer
        from buffer_segmento b
        left join intercessao_segmento i using (shape_id, id_segmento)
    ),
    indicador_validacao_shape as (
        select
            s.*,
            st_intersects(s.segmento, t.buffer_tunel) as indicador_tunel,
            st_area(s.buffer) / st_area(s.buffer_completo)
            < 0.5 as indicador_area_prejudicada,
            s.comprimento_segmento < 990 as indicador_segmento_pequeno,
            cast(id_segmento as integer) as id_segmento_int
        from buffer_segmento_recortado s
        cross join tunel t
    )
select
    * except (id_segmento_int),
    (
        (
            indicador_tunel
            and (
                (id_segmento_int > 1)
                and (
                    id_segmento_int
                    < max(id_segmento_int) over (partition by feed_start_date, shape_id)
                )
            )
        )
        or indicador_area_prejudicada
        or indicador_segmento_pequeno
    ) as indicador_segmento_desconsiderado,
    '{{ var("version") }}' as versao
from indicador_validacao_shape

{% if is_incremental() %}

    union all

    select
        s.feed_version,
        s.feed_start_date,
        fi.feed_end_date,
        s.shape_id,
        s.id_segmento,
        s.segmento,
        s.wkt_segmento,
        s.comprimento_segmento,
        s.buffer_completo,
        s.buffer,
        s.indicador_tunel,
        s.indicador_area_prejudicada,
        s.indicador_segmento_pequeno,
        s.indicador_segmento_desconsiderado,
        s.versao
    from {{ this }} s
    join {{ ref("feed_info_gtfs") }} fi using (feed_start_date)
    {# join `rj-smtr.gtfs.feed_info` fi using (feed_start_date) #}
    where feed_start_date = '{{ last_feed_version }}'

{% endif %}
