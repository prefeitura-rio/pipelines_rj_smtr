{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        alias="sppo_aux_registro_filtrada",
        require_partition_filter=true,
    )
}}
/*
Descrição:
Filtragem e tratamento básico de registros de gps.
1. Filtra registros que estão fora de uma caixa que contém a área do município de Rio de Janeiro.
2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.
3. Muda o nome de variáveis para o padrão do projeto.
	- id_veiculo --> ordem
*/
with
    box as (
        /* 1. Geometria de caixa que contém a área do município de Rio de Janeiro.*/
        select * from {{ ref("limites_geograficos_caixa") }}
    ),
    gps as (
        /* 2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.*/
        select *, st_geogpoint(longitude, latitude) posicao_veiculo_geo
        from {{ ref("sppo_registros_zirix") }}
        {% if is_incremental() -%}
            where
                data between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and timestamp_gps > "{{var('date_range_start')}}"
                and timestamp_gps <= "{{var('date_range_end')}}"
        {%- endif -%}
    ),
    realocacao as (
        select g.* except (linha), coalesce(r.servico_realocado, g.linha) as linha
        from gps g
        left join
            {{ ref("sppo_aux_registros_realocacao_zirix") }} r
            on g.ordem = r.id_veiculo
            and g.timestamp_gps = r.timestamp_gps
    ),
    filtrada as (
        /* 1,2, e 3. Muda o nome de variáveis para o padrão do projeto.*/
        select
            ordem as id_veiculo,
            latitude,
            longitude,
            posicao_veiculo_geo,
            velocidade,
            linha,
            timestamp_gps,
            timestamp_captura,
            data,
            hora,
            row_number() over (partition by ordem, timestamp_gps, linha) rn
        from realocacao
        where
            st_intersectsbox(
                posicao_veiculo_geo,
                (select min_longitude from box),
                (select min_latitude from box),
                (select max_longitude from box),
                (select max_latitude from box)
            )
    )
select * except (rn), "{{ var('version') }}" as versao
from filtrada
where rn = 1
