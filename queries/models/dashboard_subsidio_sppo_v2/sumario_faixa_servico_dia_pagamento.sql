{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    subsidio_faixa as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            viagens_faixa,
            km_planejada_faixa,
            pof
        from {{ ref("subsidio_faixa_servico_dia") }}
        -- from `rj-smtr.financeiro_staging.subsidio_faixa_servico_dia`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    penalidade as (
        select
            data,
            tipo_dia,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            valor_penalidade
        from {{ ref("subsidio_penalidade_servico_faixa") }}
        -- from `rj-smtr.financeiro.subsidio_penalidade_servico_faixa`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
    ),
    subsidio_parametros as (
        select distinct
            data_inicio,
            data_fim,
            status,
            tecnologia,
            subsidio_km,
            case
                when tecnologia is null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim
                    )
                when tecnologia is not null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim, tecnologia
                    )
            end as subsidio_km_teto
        from {{ ref("valor_km_tipo_viagem") }}
    -- from `rj-smtr.subsidio.valor_km_tipo_viagem`
    ),
    subsidio_faixa_agg as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            sum(km_apurada_faixa) as km_apurada_faixa,
            sum(km_subsidiada_faixa) as km_subsidiada_faixa,
            sum(valor_apurado) as valor_apurado,
            sum(valor_glosado_tecnologia) as valor_glosado_tecnologia,
            sum(valor_acima_limite) as valor_acima_limite,
            sum(
                valor_total_sem_glosa - valor_glosado_tecnologia
            ) as valor_total_sem_glosa,
            sum(valor_apurado) + p.valor_penalidade as valor_total_com_glosa,
            case
                when p.valor_penalidade != 0
                then - p.valor_penalidade
                else
                    safe_cast(
                        (
                            sum(
                                if(
                                    indicador_viagem_dentro_limite = true
                                    and indicador_penalidade_judicial = true,
                                    km_apurada_faixa * sp.subsidio_km_teto,
                                    0
                                )
                            ) - sum(
                                if(
                                    indicador_viagem_dentro_limite = true
                                    and indicador_penalidade_judicial = true,
                                    km_apurada_faixa * sp.subsidio_km,
                                    0
                                )
                            )
                        ) as numeric
                    )
            end as valor_judicial,
            p.valor_penalidade
        from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }} as s
        -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem` as s
        left join
            penalidade as p using (
                data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, servico
            )
        left join
            subsidio_parametros as sp
            on s.data between sp.data_inicio and sp.data_fim
            and s.tipo_viagem = sp.status
            and (
                s.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
                or (
                    s.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and (
                        s.tecnologia_remunerada = sp.tecnologia
                        or (s.tecnologia_remunerada is null and sp.tecnologia is null)
                    )
                )
                or (
                    s.data < date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and sp.tecnologia is null
                )
            )
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
        group by
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            valor_penalidade
    ),
    pivot_data as (
        select *
        from
            (
                select
                    data,
                    tipo_dia,
                    faixa_horaria_inicio,
                    faixa_horaria_fim,
                    consorcio,
                    servico,
                    case
                        when
                            tipo_viagem in (
                                "Licenciado sem ar e não autuado",
                                "Licenciado com ar e não autuado"
                            )
                            and tecnologia_apurada is not null
                        then concat(tipo_viagem, ' - ', tecnologia_apurada)
                        else tipo_viagem
                    end as tipo_viagem_tecnologia,
                    km_apurada_faixa
                from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
                -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
                where
                    data between date('{{ var("start_date") }}') and date(
                        '{{ var("end_date") }}'
                    )
            ) pivot (
                sum(km_apurada_faixa) as km_apurada for tipo_viagem_tecnologia in (
                    "Registrado com ar inoperante" as registrado_com_ar_inoperante,
                    "Não licenciado" as n_licenciado,
                    "Autuado por ar inoperante" as autuado_ar_inoperante,
                    "Autuado por segurança" as autuado_seguranca,
                    "Autuado por limpeza/equipamento" as autuado_limpezaequipamento,
                    "Licenciado sem ar e não autuado" as licenciado_sem_ar_n_autuado,
                    "Licenciado com ar e não autuado" as licenciado_com_ar_n_autuado,
                    "Licenciado sem ar e não autuado - MINI"
                    as licenciado_sem_ar_n_autuado_mini,
                    "Licenciado com ar e não autuado - MINI"
                    as licenciado_com_ar_n_autuado_mini,
                    "Licenciado sem ar e não autuado - MIDI"
                    as licenciado_sem_ar_n_autuado_midi,
                    "Licenciado com ar e não autuado - MIDI"
                    as licenciado_com_ar_n_autuado_midi,
                    "Licenciado sem ar e não autuado - BASICO"
                    as licenciado_sem_ar_n_autuado_basico,
                    "Licenciado com ar e não autuado - BASICO"
                    as licenciado_com_ar_n_autuado_basico,
                    "Licenciado sem ar e não autuado - PADRON"
                    as licenciado_sem_ar_n_autuado_padron,
                    "Licenciado com ar e não autuado - PADRON"
                    as licenciado_com_ar_n_autuado_padron,
                    "Não vistoriado" as n_vistoriado,
                    "Sem transação" as sem_transacao
                )
            )
    )
select
    s.data,
    s.tipo_dia,
    s.faixa_horaria_inicio,
    s.faixa_horaria_fim,
    s.consorcio,
    s.servico,
    s.viagens_faixa,
    agg.km_apurada_faixa,
    agg.km_subsidiada_faixa,
    s.km_planejada_faixa,
    s.pof,
    coalesce(
        km_apurada_registrado_com_ar_inoperante, 0
    ) as km_apurada_registrado_com_ar_inoperante,
    coalesce(km_apurada_n_licenciado, 0) as km_apurada_n_licenciado,
    coalesce(km_apurada_autuado_ar_inoperante, 0) as km_apurada_autuado_ar_inoperante,
    coalesce(km_apurada_autuado_seguranca, 0) as km_apurada_autuado_seguranca,
    coalesce(
        km_apurada_autuado_limpezaequipamento, 0
    ) as km_apurada_autuado_limpezaequipamento,
    coalesce(km_apurada_n_vistoriado, 0) as km_apurada_n_vistoriado,
    coalesce(km_apurada_sem_transacao, 0) as km_apurada_sem_transacao,
    coalesce(
        km_apurada_licenciado_sem_ar_n_autuado_mini, 0
    ) as km_apurada_licenciado_sem_ar_n_autuado_mini,
    coalesce(
        km_apurada_licenciado_com_ar_n_autuado_mini, 0
    ) as km_apurada_licenciado_com_ar_n_autuado_mini,
    coalesce(
        km_apurada_licenciado_sem_ar_n_autuado_midi, 0
    ) as km_apurada_licenciado_sem_ar_n_autuado_midi,
    coalesce(
        km_apurada_licenciado_com_ar_n_autuado_midi, 0
    ) as km_apurada_licenciado_com_ar_n_autuado_midi,
    coalesce(
        km_apurada_licenciado_sem_ar_n_autuado_basico, 0
    ) as km_apurada_licenciado_sem_ar_n_autuado_basico,
    coalesce(
        km_apurada_licenciado_com_ar_n_autuado_basico, 0
    ) as km_apurada_licenciado_com_ar_n_autuado_basico,
    coalesce(
        km_apurada_licenciado_sem_ar_n_autuado_padron, 0
    ) as km_apurada_licenciado_sem_ar_n_autuado_padron,
    coalesce(
        km_apurada_licenciado_com_ar_n_autuado_padron, 0
    ) as km_apurada_licenciado_com_ar_n_autuado_padron,
    case
        when s.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
        then
            coalesce(km_apurada_licenciado_sem_ar_n_autuado_mini, 0)
            + coalesce(km_apurada_licenciado_sem_ar_n_autuado_midi, 0)
            + coalesce(km_apurada_licenciado_sem_ar_n_autuado_basico, 0)
            + coalesce(km_apurada_licenciado_sem_ar_n_autuado_padron, 0)
        else coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
    end as km_apurada_total_licenciado_sem_ar_n_autuado,
    case
        when s.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
        then
            coalesce(km_apurada_licenciado_com_ar_n_autuado_mini, 0)
            + coalesce(km_apurada_licenciado_com_ar_n_autuado_midi, 0)
            + coalesce(km_apurada_licenciado_com_ar_n_autuado_basico, 0)
            + coalesce(km_apurada_licenciado_com_ar_n_autuado_padron, 0)
        else coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
    end as km_apurada_total_licenciado_com_ar_n_autuado,
    agg.valor_total_com_glosa as valor_a_pagar,
    agg.valor_glosado_tecnologia,
    agg.valor_total_com_glosa - agg.valor_total_sem_glosa as valor_total_glosado,
    agg.valor_acima_limite,
    agg.valor_total_sem_glosa,
    agg.valor_acima_limite
    + agg.valor_penalidade
    + agg.valor_total_sem_glosa as valor_total_apurado,
    agg.valor_judicial,
    agg.valor_penalidade,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from subsidio_faixa as s
left join
    subsidio_faixa_agg as agg using (
        data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, consorcio, servico
    )
left join
    pivot_data as pd using (
        data, tipo_dia, faixa_horaria_inicio, faixa_horaria_fim, consorcio, servico
    )
