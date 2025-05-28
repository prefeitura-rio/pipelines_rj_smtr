{% test teto_viagens(model) -%}
with
    os as (
        select
            data,
            servico,
            case
                when fh.tipo_os != "Regular"
                then concat(concat(fh.tipo_dia, " - "), fh.tipo_os)
                else fh.tipo_dia
            end as tipo_dia,
            safe_cast(
                concat(concat(data, "T"), faixa_horaria_inicio) as datetime
            ) as faixa_horaria_inicio,
            partidas,
        from {{ ref('ordem_servico_faixa_horaria') }} fh
        -- from `rj-smtr.planejamento.ordem_servico_faixa_horaria` fh
        right join
            {{ ref('subsidio_data_versao_efetiva') }} s
            -- `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva` s
            on s.feed_start_date = fh.feed_start_date
            and fh.tipo_dia = s.tipo_dia
            and coalesce(s.tipo_os, "Regular") = fh.tipo_os
        where
            data between "2025-04-01" and "2025-04-30"
            and partidas > 0
            and quilometragem > 0
    ),
    viagens_remuneradas as (
        select
            r.data,
            r.servico,
            datetime(faixa_horaria_inicio) as faixa_horaria_inicio,
            indicador_viagem_dentro_limite
        from
            {{ ref('viagens_remuneradas') }} r
            -- `rj-smtr-dev.abr_reprocessamento__dashboard_subsidio_sppo.viagens_remuneradas` r
        left join
            {{ ref('viagens_completa') }} c 
            -- `rj-smtr.projeto_subsidio_sppo.viagem_completa` c 
            using (data, id_viagem)
        left join
            {{ ref('viagem_planejada') }} c  p
            -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada` p
            on p.data = c.data
            and r.servico = p.servico
            and c.datetime_partida
            between datetime(faixa_horaria_inicio) and datetime(faixa_horaria_fim)
        where r.data between "2025-04-01" and "2025-04-30"
    ),
    viagens as (
        select
            * except (indicador_viagem_dentro_limite),

            countif(indicador_viagem_dentro_limite is true) as viagens_dentro_limite,
            count(*) as viagens_total
        from viagens_remuneradas
        group by 1, 2, 3
    )
select *
from viagens
left join os using (data, servico, faixa_horaria_inicio)
where viagens_total <= partidas and viagens_dentro_limite < viagens_total
{% endtest %}