{{
    config(
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}
{% set transacao_table = ref("transacao") %}
{% if execute %}
    {% if is_incremental() %}

        {% set partitions_query %}
      SELECT
        CONCAT("'", PARSE_DATE("%Y%m%d", partition_id), "'") AS data
      FROM
        `{{ transacao_table.database }}.{{ transacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = "{{ transacao_table.identifier }}"
        AND partition_id != "__NULL__"
        AND DATE(last_modified_time, "America/Sao_Paulo") = DATE_SUB(DATE("{{var('run_date')}}"), INTERVAL 1 DAY)
        {% endset %}

        {{ log("Running query: \n" ~ partitions_query, info=True) }}
        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
        {{ log("transacao partitions: \n" ~ partition_list, info=True) }}
    {% endif %}
{% endif %}

with
    transacao as (
        select
            t.data,
            t.hora,
            t.datetime_transacao,
            t.datetime_processamento,
            t.datetime_captura,
            t.modo,
            t.id_consorcio,
            t.consorcio,
            t.id_operadora,
            t.operadora,
            t.id_servico_jae,
            t.servico_jae,
            t.descricao_servico_jae,
            t.id_transacao,
            t.longitude,
            t.latitude,
            ifnull(t.longitude, 0) as longitude_tratada,
            ifnull(t.latitude, 0) as latitude_tratada,
            s.longitude as longitude_servico,
            s.latitude as latitude_servico,
            s.id_servico_gtfs,
            s.id_servico_jae as id_servico_jae_cadastro
        from {{ ref("transacao") }} t
        left join
            {{ ref("servicos") }} s
            on t.id_servico_jae = s.id_servico_jae
            and t.data >= s.data_inicio_vigencia
            and (t.data <= s.data_fim_vigencia or s.data_fim_vigencia is null)
        {% if is_incremental() %}
            where
                {% if partition_list | length > 0 %}
                    data in ({{ partition_list | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        {% endif %}
    ),
    indicadores as (
        select
            * except (
                id_servico_gtfs,
                latitude_tratada,
                longitude_tratada,
                id_servico_jae_cadastro
            ),
            latitude_tratada = 0
            or longitude_tratada = 0 as indicador_geolocalizacao_zerada,
            (
                (latitude_tratada != 0 or longitude_tratada != 0)
                and not st_intersectsbox(
                    st_geogpoint(longitude_tratada, latitude_tratada),
                    -43.87,
                    -23.13,
                    -43.0,
                    -22.59
                )
            ) as indicador_geolocalizacao_fora_rio,
            (
                latitude_tratada != 0
                and longitude_tratada != 0
                and latitude_servico is not null
                and longitude_servico is not null
                and modo = "BRT"
                and st_distance(
                    st_geogpoint(longitude_tratada, latitude_tratada),
                    st_geogpoint(longitude_servico, latitude_servico)
                )
                > 100
            ) as indicador_geolocalizacao_fora_stop,
            id_servico_gtfs is null
            and id_servico_jae_cadastro is not null
            and modo in ("Ônibus", "BRT") as indicador_servico_fora_gtfs,
            id_servico_jae_cadastro is null as indicador_servico_fora_vigencia
        from transacao
    )
select
    * except (indicador_servico_fora_gtfs, indicador_servico_fora_vigencia),
    case
        when indicador_geolocalizacao_zerada = true
        then "Geolocalização zerada"
        when indicador_geolocalizacao_fora_rio = true
        then "Geolocalização fora do município"
        when indicador_geolocalizacao_fora_stop = true
        then "Geolocalização fora do stop"
    end as descricao_geolocalizacao_invalida,
    indicador_servico_fora_gtfs,
    indicador_servico_fora_vigencia,
    '{{ var("version") }}' as versao
from indicadores
where
    indicador_geolocalizacao_zerada = true
    or indicador_geolocalizacao_fora_rio = true
    or indicador_geolocalizacao_fora_stop = true
    or indicador_servico_fora_gtfs = true
    or indicador_servico_fora_vigencia = true
