{{
    config(
        materialized="ephemeral",
    )
}}

with
    reparticao_unnest as (
        select
            data_inicio_matriz as data_inicio,
            data_fim_matriz as data_fim,
            row_number() over (partition by integracao) as idx_modo,
            integracao,
            modo,
            tempo_integracao_minutos
        from {{ ref("matriz_reparticao_tarifaria") }}, unnest(sequencia_modo) as modo
        where integracao not like "%Metrô%"

    ),
    modos_origem_destino as (
        select
            data_inicio,
            data_fim,
            integracao,
            modo as modo_origem,
            idx_modo,
            string_agg(modo, '-') over (
                partition by integracao order by idx_modo
            ) as integracao_origem,
            lead(modo) over (partition by integracao order by idx_modo) as modo_destino,
            tempo_integracao_minutos
        from reparticao_unnest
    )
select
    data_inicio,
    data_fim,
    case when idx_modo = 1 then modo_origem end as modo_origem,
    cast(null as string) as id_servico_jae_origem,
    cast(null as string) as id_servico_gtfs_origem,
    cast(null as string) as tabela_gtfs_origem,
    case when idx_modo > 1 then integracao_origem end as integracao_origem,
    modo_destino,
    cast(null as string) as id_servico_jae_destino,
    cast(null as string) as id_servico_gtfs_destino,
    cast(null as string) as tabela_gtfs_destino,
    tempo_integracao_minutos,
    cast(5.0 as numeric) as valor_integracao,
    'Integração' as tipo_integracao,
    true as indicador_integracao
from modos_origem_destino
where modo_destino is not null
