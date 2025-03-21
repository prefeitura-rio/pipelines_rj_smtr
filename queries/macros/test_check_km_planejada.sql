{% test check_km_planejada(model) -%}
    with
        viagem_planejada as (
            select distinct
                data,
                servico,
                faixa_horaria_inicio,
                round(distancia_total_planejada, 3) as distancia_total_planejada
            from {{ ref("viagem_planejada") }}
            -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        os_faixa as (
            select
                date(
                    datetime(data) + interval cast(
                        split(faixa_horaria_inicio, ':')[safe_offset(0)] as int64
                    ) hour
                ) as data,
                servico,
                datetime(data) + interval cast(
                    split(faixa_horaria_inicio, ':')[safe_offset(0)] as int64
                ) hour as faixa_horaria_inicio,
                round(sum(quilometragem), 3) as quilometragem
            from {{ ref("subsidio_data_versao_efetiva") }}
            -- `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`
            left join
                {{ ref("ordem_servico_faixa_horaria") }}
                -- `rj-smtr.planejamento.ordem_servico_faixa_horaria`
                using (feed_start_date, tipo_os, tipo_dia)
            where
                data between date_sub(
                    date("{{ var('date_range_start') }}"), interval 1 day
                ) and date("{{ var('date_range_end') }}")
            group by 1, 2, 3
        )
    select *
    from viagem_planejada p
    full join os_faixa using (data, servico, faixa_horaria_inicio)
    where
        quilometragem != distancia_total_planejada

{%- endtest %}
