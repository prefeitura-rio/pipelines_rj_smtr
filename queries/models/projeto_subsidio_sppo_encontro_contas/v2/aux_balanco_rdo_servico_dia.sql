/*

- Cenário F: exatamente como foi realizado no encontro de contas 2022-2023, adicionado os dias atípicos (pois ainda não estão 100% definidos)
- Cenário G: removidos os dias em que não houve subsídio dos serviços e
             adicionado os dias atípicos (pois ainda não estão 100% definidos)
- Cenário H: removidos os dias em que não houve subsídio dos serviços e
             adicionado os dias atípicos (pois ainda não estão 100% definidos) e
             adicionados os dias-serviço que foram subsidiados, mas não tem receita tarifária
- Cenário H1: cenário H com correções
- Cenário H2: cenário H1 + extensão até 2025-03-31 + correções serviços sem associação
- Cenário I: cenário H com correções + apenas tarifário
- Cenário J: cenário I com correções dos veículos rodoviários
- Cenário K: cenário H2 + I + J

*/
{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %} {{ config(enabled=false) }}
{% else %}
    {{
        config(
            materialized="ephemeral",
        )
    }}
{% endif %}

with
    -- 1. Calcula a receita tarifaria por servico e dia
    rdo_raw as (
        select
            data,
            consorcio,
            case
                when length(linha) < 3
                then lpad(linha, 3, "0")
                else
                    concat(
                        ifnull(regexp_extract(linha, r"[A-Z]+"), ""),
                        ifnull(regexp_extract(linha, r"[0-9]+"), "")
                    )
            end as servico,
            linha,
            tipo_servico,
            ordem_servico,
            round(
                sum(receita_buc)
                + sum(receita_buc_supervia)
                + sum(receita_cartoes_perna_unica_e_demais)
                + sum(receita_especie),
                0
            ) as receita_tarifaria_aferida
        from
            {# {{ ref("rdo40_registros") }} #}
            `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo40_registros`
        where
            data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
            {# AND DATA NOT IN ("2022-10-02", "2022-10-30", '2023-02-07', '2023-02-08', '2023-02-10', '2023-02-13', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22') #}
            and data not in ("2024-10-06")  -- Eleições 2024
            and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            and not (
                length(ifnull(regexp_extract(linha, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(linha, r"[0-9]+"), "") like "2%"
            )  -- Remove rodoviarios
        group by 1, 2, 3, 4, 5, 6
    ),
    correcao_servico_rdo as (
        select data, servico, servico_corrigido, tipo
        from {{ ref("staging_encontro_contas_correcao_servico_rdo") }}
    ),
    rdo_corrigido as (
        select *
        from rdo_raw as r
        left join
            (
                select
                    * except (servico_corrigido),
                    servico_corrigido as servico_corrigido_rdo
                from correcao_servico_rdo
                where tipo = "Sem planejamento porém com receita tarifária"
            ) using (data, servico)
    ),
    rdo_filtrado as (select * from rdo_corrigido where receita_tarifaria_aferida != 0),  -- Remove servicos nao subsidiados
    sumario_dia as (
        select data, consorcio, servico, perc_km_planejada
        from
            {# {{ ref("sumario_servico_dia_historico") }} #}
            {# `rj-smtr.monitoramento.sumario_servico_dia_historico` #}
            {{ ref("staging_encontro_contas_sumario_servico_dia_historico") }}
        where
            data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
            and data not in ("2024-10-06")  -- Eleições 2024
    {# and valor_subsidio_pago = 0 -- Desabilitar para Cenário E #}
    ),
    sumario_dia_corrigido as (
        select *
        from sumario_dia
        left join
            (
                select
                    * except (servico_corrigido),
                    servico_corrigido as servico_corrigido_sumario
                from correcao_servico_rdo
                where tipo = "Subsídio pago sem receita tarifária"
            ) using (data, servico)
    ),
    rdo_sumario as (
        select
            coalesce(sd.data, rdo.data) as data,
            coalesce(sd.consorcio, rdo.consorcio) as consorcio,
            coalesce(sd.servico, rdo.servico) as servico,
            sd.* except (data, consorcio, servico),
            rdo.* except (data, consorcio, servico)
        from sumario_dia_corrigido as sd
        full join
            rdo_filtrado as rdo
            on (
                sd.data = rdo.data
                and (
                    sd.servico = rdo.servico
                    or sd.servico = rdo.servico_corrigido_rdo
                    or sd.servico_corrigido_sumario = rdo.servico_corrigido_rdo
                    or sd.servico_corrigido_sumario = rdo.servico
                )
            )
        {# where sd.servico is null #}
        where
            (perc_km_planejada >= 80 and receita_tarifaria_aferida is null)
            or (receita_tarifaria_aferida is not null and perc_km_planejada is null)

    )  -- Cenário H/H1

select
    bsd.data,
    bsd.consorcio,
    bsd.servico,
    bsd.km_subsidiada,
    bsd.receita_tarifaria_aferida,
    rdo.data as data_rdo,
    rdo.consorcio as consorcio_rdo,
    rdo.servico as servico_tratado_rdo,
    rdo.linha as linha_rdo,
    rdo.tipo_servico as tipo_servico_rdo,
    rdo.ordem_servico as ordem_servico_rdo,
    rdo.receita_tarifaria_aferida as receita_tarifaria_aferida_rdo
from {{ ref("balanco_servico_dia" ~ var("encontro_contas_modo")) }} bsd
full join rdo_sumario rdo using (data, servico)
