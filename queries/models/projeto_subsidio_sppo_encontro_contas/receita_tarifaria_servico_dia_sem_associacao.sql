/*

- Cenário D: exatamente como foi realizado no encontro de contas 2022-2023, adicionado os dias atípicos (pois ainda não estão 100% definidos)
- Cenário E: removidos os dias em que não houve subsídio dos serviços e adicionado os dias atípicos (pois ainda não estão 100% definidos)
- Cenário E1: removidos os dias em que não houve subsídio dos serviços e
              adicionado os dias atípicos (pois ainda não estão 100% definidos) e
              adicionados os dias-serviço que foram subsidiados, mas não tem receita tarifária
- Cenário E2: cenário E1 com correções
- Cenário E3: cenário E1 + correções para remover executivos + inclusão de correções de serviços
*/
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
            AND DATA NOT IN ("2024-10-06")
            and consorcio in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            and not (
                length(ifnull(regexp_extract(linha, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(linha, r"[0-9]+"), "") like "2%"
            )  -- Remove rodoviarios
        group by 1, 2, 3, 4, 5, 6
    ),
    correcao_servico_rdo as (
        select
            * except (servico_corrigido),
            case
                when trim(justificativa) = "Lançamento da operação na linha principal."
                then regexp_extract(servico, r"[0-9]+")
                when
                    trim(justificativa)
                    like "Por orientação da SMTR, esta linha está no RDO como LCD%"
                then concat("LECD", regexp_extract(servico, r"[0-9]+"))
                when trim(servico_corrigido) = ""
                then null
                else servico_corrigido
            end as servico_corrigido
        from
            `rj-smtr-dev.rodrigo__projeto_subsidio_sppo_encontro_contas_2024_jan_ago_cenario_e3.correcao_servico_rdo`
    ),
    rdo_corrigido as (
        select * except (servico), coalesce(servico_corrigido, servico) as servico
        from rdo_raw
        left join correcao_servico_rdo using (data, servico)
    ),
    rdo_filtrado as (select * from rdo_corrigido where receita_tarifaria_aferida != 0),  -- Remove servicos nao subsidiados
    sumario_dia as (
        select
            data,
            consorcio,
            servico,
            sum(km_apurada) as km_subsidiada,
            sum(valor_subsidio_pago) as subsidio_pago
        from
            {# {{ ref("sumario_servico_dia_historico") }} #}
            `rj-smtr.monitoramento.sumario_servico_dia_historico`
        where data between "{{ var('start_date') }}" and "{{ var('end_date') }}"
        {# and valor_subsidio_pago = 0 -- Desabilitar para Cenário E #}
        group by 1, 2, 3
    )
select
    data,
    coalesce(rdo.consorcio, sd.consorcio) as consorcio,
    servico,
    linha,
    tipo_servico,
    ordem_servico,
    receita_tarifaria_aferida
from rdo_filtrado as rdo
{# left join sumario_dia sd #}
full join
    sumario_dia sd  -- Cenário E1/E2
    using (data, servico)
{# where sd.servico is null #}
where
    (
        (subsidio_pago > 0 and receita_tarifaria_aferida is null)
        or (receita_tarifaria_aferida is not null and subsidio_pago is null)
    )  -- Cenário E1/E2
