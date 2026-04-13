{{ config(materialized="ephemeral") }}

with
    subsidio_dia as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}') and rn = 1
                then min_pof
                when data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                then pof
                else null
            end as min_pof
        from
            (
                select
                    *,
                    min(pof) over (
                        partition by data, tipo_dia, consorcio, servico, sentido
                    ) as min_pof,
                    row_number() over (
                        partition by data, tipo_dia, consorcio, servico, sentido
                        order by pof, faixa_horaria_inicio
                    ) as rn
                from {{ ref("percentual_operacao_faixa_horaria") }}
                -- from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
                {% if is_incremental() %}
                    where
                        data between date('{{ var("start_date") }}') and date(
                            '{{ var("end_date") }}'
                        )
                {% endif %}
            )
    ),
    penalidade as (
        select
            data_inicio,
            data_fim,
            perc_km_inferior,
            perc_km_superior,
            ifnull(- valor, 0) as valor_penalidade
        from {{ ref("valor_tipo_penalidade") }}
    -- from `rj-smtr.dashboard_subsidio_sppo.valor_tipo_penalidade`
    )
select
    s.data,
    s.tipo_dia,
    s.consorcio,
    s.servico,
    s.sentido,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    case
        when
            (
                s.servico in (
                    "300",
                    "SR300",
                    "324",
                    "SN324",
                    "326",
                    "328",
                    "SN328",
                    "SR328",
                    "329",
                    "335",
                    "342",
                    "SR342",
                    "349",
                    "355",
                    "SN355",
                    "SR355",
                    "362",
                    "SR362",
                    "369",
                    "378",
                    "SR378",
                    "384",
                    "SR384",
                    "SV384",
                    "385",
                    "386",
                    "SR386",
                    "388",
                    "SN388",
                    "SR388",
                    "393",
                    "SN393",
                    "SR393",
                    "397",
                    "SN397",
                    "SR397",
                    "399",
                    "SN399",
                    "SR399",
                    "SPB483",
                    "634",
                    "SN634",
                    "665",
                    "SVA665",
                    "SVB665",
                    "696",
                    "SN696",
                    "SVB901"
                )
                and s.data = "2026-03-09"
            )
            or (
                s.servico in (
                    "292",
                    "300",
                    "SR300",
                    "315",
                    "324",
                    "SN324",
                    "326",
                    "328",
                    "SN328",
                    "SR328",
                    "329",
                    "335",
                    "338",
                    "342",
                    "SR342",
                    "343",
                    "SP343",
                    "348",
                    "349",
                    "352",
                    "355",
                    "SN355",
                    "SR355",
                    "361",
                    "362",
                    "SR362",
                    "369",
                    "378",
                    "SR378",
                    "380",
                    "384",
                    "SR384",
                    "SV384",
                    "385",
                    "386",
                    "SR386",
                    "388",
                    "SN388",
                    "SR388",
                    "393",
                    "SN393",
                    "SR393",
                    "397",
                    "SN397",
                    "SR397",
                    "399",
                    "SN399",
                    "SR399",
                    "483",
                    "SN483",
                    "SPA483",
                    "SPB483",
                    "SV483",
                    "484",
                    "486",
                    "497",
                    "SN497",
                    "498",
                    "665",
                    "SVA665",
                    "SVB665",
                    "906"
                )
                and s.data = "2026-03-10"
            )
            or (
                s.servico
                in ("201", "202", "006", "133", "507", "426", "607", "711", "416")
                and s.data = "2026-03-18"
            )
            or (s.servico in ("133", "607", "711") and s.data = "2026-03-19")
            or (s.servico in ("133", "607") and s.data = "2026-03-20")
            or (s)
        then 0  -- Processo SEI_000301.005390_2026_67
        else safe_cast(coalesce(pe.valor_penalidade, 0) as numeric)
    end as valor_penalidade,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao

from subsidio_dia as s
left join
    penalidade as pe
    on s.data between pe.data_inicio and pe.data_fim
    and s.min_pof >= pe.perc_km_inferior
    and s.min_pof < pe.perc_km_superior
