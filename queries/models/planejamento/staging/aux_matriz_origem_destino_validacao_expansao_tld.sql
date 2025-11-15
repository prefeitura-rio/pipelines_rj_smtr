/*
Documentação do Modelo: validacao_expansao_e_tld
======================================================
1. Propósito do Modelo
-----------------------
Este modelo calcula métricas de qualidade sobre a etapa de EXPANSÃO da matriz OD.
Ele avalia duas coisas:
1. O "esforço" da expansão, analisando a distribuição dos fatores de expansão.
2. O impacto da expansão na Dist. de Comprimento de Viagem (Trip Length Distribution).

A saída é uma tabela no formato (metrica, valor, descricao), pronta para ser
consumida por um modelo de relatório final.

2. Entradas
-----------
- `aux_matriz_origem_destino_bruta`: A matriz OD antes da expansão.
- `aux_matriz_origem_destino_expandida_origem`: A matriz OD após a expansão por origem.
- `aux_embarques_reais_por_hex`: Totais reais de embarques por hex.
- `h3_res8`: Tabela com a geometria dos hexágonos para cálculo de distância.
*/
with
    -- Recalcula os fatores de expansão para análise
    embarques_observados as (
        select origem_id, sum(total_viagens) as total_viagens_observadas
        from {{ ref("aux_matriz_origem_destino_bruta") }}
        group by origem_id
    ),
    embarques_reais as (
        select
            tile_id as origem_id, sum(quantidade_embarques) as quantidade_embarques_real
        from {{ ref("aux_passageiro_dia_hex") }}
        group by origem_id
    ),
    fatores_de_expansao as (
        select
            obs.origem_id,
            safe_divide(
                real.quantidade_embarques_real, obs.total_viagens_observadas
            ) as fator_expansao
        from embarques_observados as obs
        join embarques_reais as real using (origem_id)
    ),
    -- Calcula as estatísticas dos fatores de expansão
    stats_fatores as (
        select
            avg(fator_expansao) as media,
            stddev(fator_expansao) as desvio_padrao,
            min(fator_expansao) as minimo,
            max(fator_expansao) as maximo
        from fatores_de_expansao
    ),
    -- Prepara dados para cálculo de distância
    distancias_hex as (
        select
            h1.tile_id as origem_id,
            h2.tile_id as destino_id,
            -- Distância em KM
            st_distance(h1.geometry_hex, h2.geometry_hex) / 1000 as distancia_km
        from {{ ref("h3_res8") }} as h1
        cross join {{ ref("h3_res8") }} as h2
    ),
    -- Calcula TLD da matriz bruta
    tld_bruta as (
        select
            sum(m.total_viagens * d.distancia_km)
            / sum(m.total_viagens) as tld_media_bruta_km
        from {{ ref("aux_matriz_origem_destino_bruta") }} as m
        join distancias_hex as d using (origem_id, destino_id)
    ),
    -- Calcula TLD da matriz calibrada (usando o modelo final)
    tld_calibrada as (
        select
            sum(m.viagens_calibradas_dia * d.distancia_km)
            / sum(m.viagens_calibradas_dia) as tld_media_calibrada_km
        from {{ ref("matriz_origem_destino_calibrada") }} as m  -- Depende do modelo Python final
        join distancias_hex as d using (origem_id, destino_id)
    )

-- Unifica todas as métricas em uma única tabela
select
    'fator_expansao_medio' as metrica,
    round(media, 2) as valor,
    'Em média, cada viagem observada representa '
    || cast(round(media, 2) as string)
    || ' viagens totais.' as descricao
from stats_fatores
union all
select
    'fator_expansao_stddev' as metrica,
    round(desvio_padrao, 2) as valor,
    'O desvio padrão dos fatores de expansão entre as zonas.' as descricao
from stats_fatores
union all
select
    'fator_expansao_minimo' as metrica,
    round(minimo, 2) as valor,
    'O menor fator de expansão aplicado a uma zona.' as descricao
from stats_fatores
union all
select
    'fator_expansao_maximo' as metrica,
    round(maximo, 2) as valor,
    'O maior fator de expansão, indicando zonas com menor representatividade na amostra.'
    as descricao
from stats_fatores
union all
select
    'tld_media_bruta_km' as metrica,
    round(tld_media_bruta_km, 2) as valor,
    'Comprimento médio de viagem (em km) na amostra de dados brutos.' as descricao
from tld_bruta
union all
select
    'tld_media_calibrada_km' as metrica,
    round(tld_media_calibrada_km, 2) as valor,
    'Comprimento médio de viagem (em km) na matriz final calibrada.' as descricao
from tld_calibrada
