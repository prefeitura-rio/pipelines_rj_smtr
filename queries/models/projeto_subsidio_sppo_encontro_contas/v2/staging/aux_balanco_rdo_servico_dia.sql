{# {{
    config(
        materialized="ephemeral",
    )
}} #}
-- Datas que serão desconsideradas no encontro de contas juntamente com o motivo
{% set datas_excecoes_dict = {
    "2024-10-06": "Eleições 2024",
} %}

-- Referências
{# {% set rdo40_registros = ref("rdo40_registros") %} #}
{% set rdo40_registros = "rj-smtr.br_rj_riodejaneiro_rdo.rdo40_registros" %}

with
    -- 1. Lista pares dia-serviço atípicos (recurso pago/em avaliação e ainda não
    -- incorporados
    -- ao data lakehouse)
    servico_dia_atipico as (
        select distinct data, servico
        from {{ ref("recurso_encontro_contas") }}
        where incorporado_datalakehouse is not true
    ),
    -- 2. Calcula a receita tarifária para cada par dia-serviço
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
        from {{ rdo40_registros }}
        where
            data >= "{{ var('DATA_SUBSIDIO_V9_INICIO') }}"  -- Válido apenas a partir da data de início da apuração por faixa horária
            and data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
            and data not in ("{{ datas_excecoes_dict.keys() | join(", ") }}")  -- Remove datas de exceção que serão desconsideradas no encontro de contas
            and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            and not (
                length(ifnull(regexp_extract(linha, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(linha, r"[0-9]+"), "") like "2%"
            )  -- Remove rodoviários
        group by all
    ),
    -- 3. Lista pares dia-serviço com correção do serviço (serviço corrigido é o
    -- serviço correto que deve ser utilizado no encontro de contas)
    correcao_servico_rdo as (select * from {{ ref("correcao_servico_rdo") }}),
    -- 4. Remove pares dia-serviço sem receita tarifária aferida
    rdo_filtrado as (select * from rdo_raw where receita_tarifaria_aferida != 0),
    -- 5. Associa serviço corrigido aos pares dia-serviço do RDO
    rdo_corrigido as (
        select *
        from rdo_filtrado as r
        left join
            (
                select
                    * except (servico_corrigido, tipo),
                    servico_corrigido as servico_corrigido_rdo
                from correcao_servico_rdo
                where tipo = "Sem planejamento porém com receita tarifária"
            ) using (data, servico)
    ),
    -- 6. Lista pares dia-serviço subsidiados
    sumario_dia as (
        select data, consorcio, servico, km_apurada, perc_km_planejada
        from {{ ref("encontro_contas_sumario_servico_dia_historico") }}
        where
            data >= "{{ var('DATA_SUBSIDIO_V9_INICIO') }}"  -- Válido apenas a partir da data de início da apuração por faixa horária
            and data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
            and data not in ("{{ datas_excecoes_dict.keys() | join(", ") }}")  -- Remove datas de exceção que serão desconsideradas no encontro de contas
    ),
    -- 7. Filtra apenas pares dia-serviço com POD >= 80%
    sumario_dia_filtrado as (select * from sumario_dia where perc_km_planejada >= 80),
    -- 8. Associa serviço corrigido aos pares dia-serviço subsidiados
    sumario_dia_corrigido as (
        select *
        from sumario_dia_filtrado
        left join
            (
                select
                    * except (servico_corrigido, tipo),
                    servico_corrigido as servico_corrigido_sumario
                from correcao_servico_rdo
                where tipo = "Subsídio pago sem receita tarifária"
            ) using (data, servico)
    ),
    -- 9. Associa pares dia-serviço subsidiados aos pares dia-serviço de receita
    -- tarifária
    sumario_dia_rdo as (
        select
            coalesce(sd.data, rdo.data) as data,
            coalesce(sd.consorcio, rdo.consorcio) as consorcio,
            sd.servico as servico_sumario,
            rdo.servico as servico_rdo,
            sd.* except (data, consorcio, servico),
            rdo.* except (data, consorcio, servico)
        from sumario_dia_corrigido as sd
        full join
            rdo_corrigido as rdo
            on (
                sd.data = rdo.data
                and (
                    sd.servico = rdo.servico
                    or sd.servico = rdo.servico_corrigido_rdo
                    or sd.servico_corrigido_sumario = rdo.servico_corrigido_rdo
                    or sd.servico_corrigido_sumario = rdo.servico
                )
            )
    )
-- 10. Remove pares dia-serviço atípicos
select s.*, sda.data is not null as indicador_atipico
from sumario_dia_rdo as s
left join
    servico_dia_atipico as sda on s.data = sda.data and s.servico_sumario = sda.servico
