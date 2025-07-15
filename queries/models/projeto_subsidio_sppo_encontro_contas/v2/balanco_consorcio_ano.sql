{{
    config(
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2024, "end": 2100, "interval": 1},
        },
    )
}}

with
    balanco_agg_raw as (
        select
            extract(year from data) as ano,
            consorcio,

            -- contagens
            sum(datas_servico) as datas_servico,
            sum(datas_servico_pod_menor_80) as datas_servico_pod_menor_80,

            -- prioridade 1: data de exceção (POD ≥ 80 %)
            sum(
                case
                    when indicador_data_excecao then datas_servico_pod_maior_80 else 0
                end
            ) as datas_servico_excecao,

            -- prioridade 2: atípico (POD ≥ 80 %, com receita)
            sum(
                case
                    when not indicador_data_excecao and indicador_atipico
                    then datas_servico_pod_maior_80
                    else 0
                end
            ) as datas_servico_atipicos,

            -- prioridade 3: ausência de receita (POD ≥ 80 %, sem receita)
            sum(
                case
                    when
                        not indicador_data_excecao
                        and not indicador_atipico
                        and indicador_ausencia_receita_tarifaria
                    then datas_servico_pod_maior_80
                    else 0
                end
            ) as datas_servico_ausencia_receita_tarifaria,

            -- agregações financeiras
            sum(km_apurada) as km_apurada,
            sum(receita_total_esperada) as receita_total_esperada,
            sum(receita_tarifaria_esperada) as receita_tarifaria_esperada,
            sum(subsidio_esperado) as subsidio_esperado,
            sum(subsidio_glosado) as subsidio_glosado,
            sum(receita_total_aferida) as receita_total_aferida,
            sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
            sum(valor_subsidio_pago) as subsidio_pago,
            sum(saldo) as saldo
        from {{ ref("aux_balanco_servico_dia") }}
        group by 1, 2
    ),

    balanco_agg as (
        select
            *,
            -- dias típicos = total - demais categorias
            datas_servico
            - datas_servico_pod_menor_80
            - datas_servico_excecao
            - datas_servico_atipicos
            - datas_servico_ausencia_receita_tarifaria as datas_servico_tipicos,
        from balanco_agg_raw
    )

select
    ano,
    consorcio,

    -- contagens
    datas_servico,
    datas_servico_pod_menor_80,
    datas_servico_excecao,
    datas_servico_atipicos,
    datas_servico_ausencia_receita_tarifaria,
    datas_servico_tipicos,

    -- percentuais
    round(
        safe_divide(datas_servico_pod_menor_80, datas_servico) * 100, 2
    ) as percentual_datas_servico_pod_menor_80,
    round(
        safe_divide(datas_servico_excecao, datas_servico) * 100, 2
    ) as percentual_datas_servico_excecao,
    round(
        safe_divide(datas_servico_atipicos, datas_servico) * 100, 2
    ) as percentual_datas_servico_atipicos,
    round(
        safe_divide(datas_servico_ausencia_receita_tarifaria, datas_servico) * 100, 2
    ) as percentual_datas_servico_ausencia_receita,
    round(
        safe_divide(datas_servico_tipicos, datas_servico) * 100, 2
    ) as percentual_datas_servico_tipicos,

    -- financeiros
    km_apurada,
    receita_total_esperada,
    receita_tarifaria_esperada,
    subsidio_esperado,
    subsidio_glosado,
    receita_total_aferida,
    receita_tarifaria_aferida,
    subsidio_pago,
    saldo

from balanco_agg
