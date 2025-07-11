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
                        split(faixa_horaria_inicio, ":")[safe_offset(0)] as int64
                    ) hour
                ) as data,
                servico,
                datetime(data) + interval cast(
                    split(faixa_horaria_inicio, ":")[safe_offset(0)] as int64
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
                and partidas > 0
                and quilometragem > 0
            group by 1, 2, 3
        )
        {% if "viagem_planejada" not in model %}
            ,
            sumario as (
                select data, servico, faixa_horaria_inicio, km_planejada_faixa
                from {{ model }}
                -- `rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento`
                where
                    data between date("{{ var('date_range_start') }}") and date(
                        "{{ var('date_range_end') }}"
                    )
            )
        {% endif %}
    select *
    from viagem_planejada p
    full join
        (
            select
                * except (servico),
                -- alteração referente ao Processo.rio MTR-CAP-2025/06098
                case
                    when faixa_horaria_inicio = "2025-06-01T00:00:00"
                    then
                        case
                            when servico = "LECD108"
                            then "LECD112"
                            when servico = "864"
                            then "LECD122"
                            else servico
                        end
                    else servico
                end as servico,
            from os_faixa
            where quilometragem != 0
        ) using (data, servico, faixa_horaria_inicio)
    {% if "viagem_planejada" not in model %}
        full join sumario using (data, servico, faixa_horaria_inicio)
    {% endif %}
    where
        quilometragem != distancia_total_planejada
        {% if "viagem_planejada" not in model %}
            or distancia_total_planejada != km_planejada_faixa
        {% endif %}
{%- endtest %}
