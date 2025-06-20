{{ config(materialized="ephemeral") }}

with
    feed_start_date as (
        select
            feed_start_date,
            feed_start_date as data_inicio,
            coalesce(
                date_sub(
                    lead(feed_start_date) over (order by feed_start_date),
                    interval 1 day
                ),
                last_day(feed_start_date, month)
            ) as data_fim
        from
            (select distinct feed_start_date from {{ ref("aux_ordem_servico_diaria") }})
    ),
    ordem_servico_pivot as (
        select *
        from
            {{ ref("aux_ordem_servico_diaria") }} pivot (
                max(partidas_ida) as partidas_ida,
                max(partidas_volta) as partidas_volta,
                max(viagens_planejadas) as viagens_planejadas,
                max(distancia_total_planejada) as km for
                tipo_dia in (
                    'Dia Útil' as du,
                    'Ponto Facultativo' as pf,
                    'Sabado' as sab,
                    'Domingo' as dom
                )
            )
    ),
    subsidio_feed_start_date_efetiva as (
        select
            data, split(tipo_dia, " - ")[0] as tipo_dia, tipo_dia as tipo_dia_original
        from {{ ref("subsidio_data_versao_efetiva") }}
    )
select
    data,
    tipo_dia_original as tipo_dia,
    servico,
    vista,
    consorcio,
    sentido,
    case
        {% set tipo_dia = {
            "Dia Útil": "du",
            "Ponto Facultativo": "pf",
            "Sabado": "sab",
            "Domingo": "dom",
        } %}
        {% set sentido = {"ida": ("I", "C"), "volta": "V"} %}
        {%- for key_s, value_s in sentido.items() %}
            {%- for key_td, value_td in tipo_dia.items() %}
                when
                    sentido
                    {% if key_s == "ida" %} in {{ value_s }}
                    {% else %} = "{{ value_s }}"
                    {% endif %} and tipo_dia = "{{ key_td }}"
                then
                    {% if key_td in ["Sabado", "Domingo"] %}
                        round(
                            safe_divide(
                                (partidas_{{ key_s }}_du * km_{{ value_td }}), km_du
                            )
                        )
                    {% else %} partidas_{{ key_s }}_{{ value_td }}
                    {% endif %}
            {% endfor -%}
        {% endfor -%}
    end as viagens_planejadas,
    horario_inicio as inicio_periodo,
    horario_fim as fim_periodo
from
    unnest(
        generate_date_array(
            (select min(data_inicio) from feed_start_date),
            (select max(data_fim) from feed_start_date)
        )
    ) as data
left join feed_start_date as d on data between d.data_inicio and d.data_fim
left join subsidio_feed_start_date_efetiva as sd using (data)
left join ordem_servico_pivot as o using (feed_start_date)
left join {{ ref("servicos_sentido") }} using (feed_start_date, servico)
where data < "{{ var('data_inicio_trips_shapes') }}"
