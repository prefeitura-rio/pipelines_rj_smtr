{{
    config(
        materialized="table",
        partition_by={
            "field": "data_inicio_matriz",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    matriz_staging as (
        select
            date(data_versao_matriz) as data_versao_matriz,
            id_tipo_integracao,
            primeira_perna,
            porcentagem_primeira_perna,
            segunda_perna,
            porcentagem_segunda_perna,
            terceira_perna,
            porcentagem_terceira_perna,
            cast(tempo_integracao_minutos as float64) as tempo_integracao_minutos
        from {{ source("source_smtr", "matriz_integracao") }}
    ),
    data_versao as (
        select distinct data_versao_matriz as data_inicio_matriz from matriz_staging
    ),
    data_fim as (
        select
            data_inicio_matriz,
            date_sub(
                lead(data_inicio_matriz) over (order by data_inicio_matriz),
                interval 1 day
            ) as data_fim_matriz
        from data_versao
    ),
    matriz as (
        select
            mi.data_versao_matriz as data_inicio_matriz,
            mi.id_tipo_integracao as id_matriz_integracao,
            p.sequencia_integracao,
            if(trim(p.modo) = '', null, p.modo) as modo,
            cast(
                if(
                    trim(p.porcentagem_rateio) = '', null, p.porcentagem_rateio
                ) as numeric
            ) as porcentagem_rateio,
            concat(
                primeira_perna,
                ', ',
                segunda_perna,
                if(terceira_perna is not null and trim(terceira_perna) != '', ', ', ''),
                terceira_perna
            ) as sequencia_completa_integracao,
            mi.tempo_integracao_minutos
        from
            matriz_staging mi,
            unnest(
                [
                    struct(
                        primeira_perna as modo,
                        porcentagem_primeira_perna as porcentagem_rateio,
                        1 as sequencia_integracao
                    ),
                    struct(
                        segunda_perna as modo,
                        porcentagem_segunda_perna as porcentagem_rateio,
                        2 as sequencia_integracao
                    ),
                    struct(
                        terceira_perna as modo,
                        porcentagem_terceira_perna as porcentagem_rateio,
                        3 as sequencia_integracao
                    )
                ]
            ) p
    )
select
    data_inicio_matriz,
    d.data_fim_matriz,
    m.* except (data_inicio_matriz),
    '{{ var("version") }}' as versao
from matriz m
join data_fim d using (data_inicio_matriz)
where modo is not null
