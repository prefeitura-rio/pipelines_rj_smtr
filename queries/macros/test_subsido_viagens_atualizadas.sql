{% test subsidio_viagens_atualizadas(model) -%}
WITH
                viagem_completa AS (
                SELECT
                    data,
                    datetime_ultima_atualizacao,
                    feed_start_date
                FROM
                    -- rj-smtr.projeto_subsidio_sppo.viagem_completa
                    {{ ref('viagem_completa') }}
                    left join {{ ref('subsidio_data_versao_efetiva')}}
                    using (data)
                WHERE
                    DATA >= "{{ var('DATA_SUBSIDIO_V6_INICIO') }}"
                    AND DATA BETWEEN DATE("{{ var('start_date') }}")
                    AND DATE("{{ var('end_date') }}")),
                sumario_servico_dia_historico AS (
                SELECT
                    data,
                    datetime_ultima_atualizacao
                FROM
                    {{ model }}
                WHERE
                    DATA BETWEEN DATE("{{ var('start_date') }}")
                    AND DATE("{{ var('end_date') }}")),
                feed_info as (
                    select feed_start_date, feed_update_datetime
                    from {{ ref('feed_info_gtfs')}}
                )
                SELECT
                DISTINCT DATA
                FROM
                viagem_completa as c
                LEFT JOIN
                sumario_servico_dia_historico AS h
                USING
                (DATA)
                left join feed_info as f
                using(feed_start_date)
                WHERE
                c.datetime_ultima_atualizacao > h.datetime_ultima_atualizacao
                and c.datetime_ultima_atualizacao > f.feed_update_datetime
{%- endtest %}