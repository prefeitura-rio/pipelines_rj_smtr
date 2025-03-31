{% if var("encontro_contas_modo") == "" %}
    -- 0. Lista servicos e dias atípicos (pagos por recurso)
    with
        recursos as (
            select
                data,
                id_recurso,
                tipo_recurso,
                -- consorcio,
                servico,
                incorporado_algoritmo,
                sum(valor_pago) as valor_pago
            from {{ ref("recursos_sppo_servico_dia_pago") }}
            {# from `rj-smtr`.`br_rj_riodejaneiro_recursos`.`recursos_sppo_servico_dia_pago` #}
            group by 1, 2, 3, 4, 5
        ),
        servico_dia_atipico as (
            select distinct data, servico
            from recursos
            where
                -- Quando o valor do recurso pago for R$ 0, desconsidera-se o recurso,
                -- pois:
                -- Recurso pode ter sido cancelado (pago e depois revertido)
                -- Problema reporto não gerou impacto na operação (quando aparece
                -- apenas 1 vez)
                valor_pago != 0
                -- Desconsideram-se recursos do tipo "Algoritmo" (igual a apuração em
                -- produção, levantado pela TR/SUBTT/CMO)
                -- Desconsideram-se recursos do tipo "Viagem Individual" (não afeta
                -- serviço-dia)
                -- Desconsideram-se recursos do tipo "Encontro de contas" (não afeta
                -- serviço-dia)
                and tipo_recurso
                not in ("Algoritmo", "Viagem Individual", "Encontro de contas")
                -- Desconsideram-se recursos de reprocessamento que já constam em
                -- produção
                and not (
                    data between "2022-06-01" and "2022-06-30"
                    and tipo_recurso = "Reprocessamento"
                )
                and not incorporado_algoritmo
        ),

        -- 1. Calcula a km subsidiada por servico e dia
        sumario_dia as (  -- Km apurada por servico e dia
            select
                data,
                consorcio,
                servico,
                sum(km_apurada) as km_subsidiada,
                sum(valor_subsidio_pago) as subsidio_pago
            from {{ ref("staging_encontro_contas_sumario_servico_dia_historico") }}
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
                and valor_subsidio_pago > 0  -- No cenário B estão sendo consideradas apenas as faixas com POF >= 80%, logo, essa verificação aqui é desnecessária
            group by 1, 2, 3
        ),
        viagem_remunerada as (  -- Km subsidiada pos regra do teto de 120% por servico e dia
            select data, servico, sum(distancia_planejada) as km_subsidiada
            {# from {{ ref("viagens_remuneradas") }} #}
            from `rj-smtr.dashboard_subsidio_sppo.viagens_remuneradas`
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
                and indicador_viagem_dentro_limite = true  -- useless
                and tipo_viagem not in ("Não licenciado", "Não vistoriado")
            group by 1, 2
        ),
        km_subsidiada_dia as (
            select
                sd.* except (km_subsidiada),
                ifnull(
                    case
                        when data >= "2023-09-16"
                        then vr.km_subsidiada
                        else sd.km_subsidiada
                    end,
                    0
                ) as km_subsidiada
            from sumario_dia sd
            left join viagem_remunerada as vr using (data, servico)
        ),

        -- 2. Filtra km subsidiada apenas em dias típicos (remove servicos e dias
        -- pagos por recurso)
        km_subsidiada_filtrada as (
            select ksd.*
            from km_subsidiada_dia ksd
            left join servico_dia_atipico sda using (data, servico)
            where
                sda.data is null
                -- Demais dias que não foi considerada a km apurada via GPS:
                and ksd.data not in (
                    "2022-10-02",
                    "2022-10-30",
                    '2023-02-07',
                    '2023-02-08',
                    '2023-02-10',
                    '2023-02-13',
                    '2023-02-17',
                    '2023-02-18',
                    '2023-02-19',
                    '2023-02-20',
                    '2023-02-21',
                    '2023-02-22'
                )
        ),

        -- 3. Calcula a receita tarifaria por servico e dia
        rdo as (
            select
                data,
                consorcio,
                case
                    when length(linha) < 3
                    then lpad(linha, 3, "0")
                    else
                        concat(
                            ifnull(regexp_extract(linha, r"[B-Z]+"), ""),
                            ifnull(regexp_extract(linha, r"[0-9]+"), "")
                        )
                end as servico,
                round(
                    sum(receita_buc)
                    + sum(receita_buc_supervia)
                    + sum(receita_cartoes_perna_unica_e_demais)
                    + sum(receita_especie),
                    0
                ) as receita_tarifaria_aferida
            {# from {{ ref("rdo40_registros") }} #}
            from `rj-smtr`.`br_rj_riodejaneiro_rdo`.`rdo40_registros`
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
                and data not in (
                    "2022-10-02",
                    "2022-10-30",
                    '2023-02-07',
                    '2023-02-08',
                    '2023-02-10',
                    '2023-02-13',
                    '2023-02-17',
                    '2023-02-18',
                    '2023-02-19',
                    '2023-02-20',
                    '2023-02-21',
                    '2023-02-22'
                )
                and consorcio
                in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            group by 1, 2, 3
        ),
        parametros_raw as (
            select
                data_inicio,
                data_fim,
                irk,
                subsidio_km,
                (
                    (
                        max(subsidio_km) over (
                            partition by date_trunc(data_inicio, year), data_fim
                        )
                    )
                    - subsidio_km
                ) as desconto_subsidio_km
            from `rj-smtr.subsidio.valor_km_tipo_viagem`
            {# from {{ source("projeto_subsidio_sppo_encontro_contas", "parametros_km") }} #}
            where
                data_inicio >= (
                    date_trunc(date("{{ var('start_date') }}"), year)
                    - interval 1 year
                    - interval 10 day
                )
                and data_fim <= (
                    date_trunc(date("{{ var('end_date') }}"), year)
                    + interval 1 year
                    + interval 10 day
                )
                and subsidio_km > 0
        ),
        parametros_treated as (
            select distinct
                data_inicio,
                data_fim,
                irk,
                case
                    when data_fim <= "2022-12-31"
                    then irk - subsidio_km  -- subsidio varia ao longo dos meses
                    else irk - (subsidio_km + desconto_subsidio_km)
                end as irk_tarifa_publica,
                (subsidio_km + desconto_subsidio_km) as subsidio_km,
                date_diff(data_fim, data_inicio, day) as dias
            from parametros_raw
        ),
        parametros as (
            select * except (dias)
            from parametros_treated as pt
            qualify
                row_number() over (
                    partition by
                        (
                            select min(p2.data_inicio)
                            from parametros_treated p2
                            where
                                p2.data_inicio < pt.data_fim
                                and p2.data_fim > pt.data_inicio
                        )
                    order by dias desc
                )
                = 1  -- Remove sobreposições temporais, mantendo os maiores períodos
        )
    select
        *,
        ifnull(receita_total_aferida, 0)
        - ifnull(receita_total_esperada - subsidio_glosado, 0) as saldo
    from
        (
            select
                ks.* except (subsidio_pago),
                ks.km_subsidiada * par.irk as receita_total_esperada,
                ks.km_subsidiada * par.irk_tarifa_publica as receita_tarifaria_esperada,
                ks.km_subsidiada * par.subsidio_km as subsidio_esperado,
                case
                    when data >= "2023-01-01"
                    then (ks.km_subsidiada * par.subsidio_km - subsidio_pago)
                    else 0
                end as subsidio_glosado,
                ifnull(rdo.receita_tarifaria_aferida, 0)
                + ifnull(ks.subsidio_pago, 0) as receita_total_aferida,
                rdo.receita_tarifaria_aferida,
                ks.subsidio_pago
            from km_subsidiada_filtrada ks
            left join rdo using (data, servico, consorcio)
            left join parametros par on ks.data between data_inicio and data_fim
        )
{% else %} {{ config(enabled=false) }}
{% endif %}
