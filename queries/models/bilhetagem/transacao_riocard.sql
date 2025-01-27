{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        schema="br_rj_riodejaneiro_bilhetagem",
    )
}}

{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
  AND timestamp_captura BETWEEN DATETIME("{{var('date_range_start')}}") AND DATETIME("{{var('date_range_end')}}")
{% endset %}

{% set transacao_staging = ref("staging_transacao_riocard") %}
{% if execute and is_incremental() %}
    {% set transacao_partitions_query %}
    SELECT DISTINCT
      CONCAT("'", DATE(data_transacao), "'") AS data_transacao
    FROM
      {{ transacao_staging }}
    WHERE
      {{ incremental_filter }}
    {% endset %}

    {% set transacao_partitions = (
        run_query(transacao_partitions_query).columns[0].values()
    ) %}
{% endif %}

with
    staging_transacao as (
        select *
        from {{ transacao_staging }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    novos_dados as (
        select
            extract(date from t.data_transacao) as data,
            extract(hour from t.data_transacao) as hora,
            t.id as id_transacao,
            t.data_transacao as datetime_transacao,
            t.data_processamento as datetime_processamento,
            t.timestamp_captura as datetime_captura,
            coalesce(do.modo, dc.modo) as modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            t.cd_linha as id_servico_jae,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            t.sentido,
            case
                when do.modo = "VLT"
                then substring(t.veiculo_id, 1, 3)
                when do.modo = "BRT"
                then null
                else t.veiculo_id
            end as id_veiculo,
            t.numero_serie_validador as id_validador,
            t.latitude_trx as latitude,
            t.longitude_trx as longitude,
            st_geogpoint(t.longitude_trx, t.latitude_trx) as geo_point_transacao,
            t.valor_transacao
        from staging_transacao t
        left join {{ ref("operadoras") }} do on t.cd_operadora = do.id_operadora_jae
        left join {{ ref("staging_linha") }} l on t.cd_linha = l.cd_linha
        left join
            {{ ref("staging_linha_consorcio") }} lc
            on t.cd_linha = lc.cd_linha
            and (
                t.data_transacao between lc.dt_inicio_validade and lc.dt_fim_validade
                or lc.dt_fim_validade is null
            )
        left join {{ ref("consorcios") }} dc on lc.cd_consorcio = dc.id_consorcio_jae
    ),
    -- consorcios AS (
    -- SELECT
    -- t.data,
    -- t.hora,
    -- t.datetime_transacao,
    -- t.datetime_processamento,
    -- t.datetime_captura,
    -- COALESCE(t.modo, dc.modo) AS modo,
    -- dc.id_consorcio,
    -- dc.consorcio,
    -- t.id_operadora,
    -- t.operadora,
    -- t.id_servico_jae,
    -- t.servico_jae,
    -- t.descricao_servico_jae,
    -- t.sentido,
    -- t.id_veiculo,
    -- t.id_validador,
    -- t.id_transacao,
    -- t.latitude,
    -- t.longitude,
    -- t.valor_transacao
    -- FROM
    -- novos_dados t
    -- LEFT JOIN
    -- {{ ref("consorcios") }} dc
    -- USING(id_consorcio_jae)
    -- ),
    particoes_completas as (
        select *, 0 as priority
        from novos_dados

        {% if is_incremental() and transacao_partitions | length > 0 %}
            union all

            select * except (versao), 1 as priority
            from {{ this }}
            where data in ({{ transacao_partitions | join(", ") }})
        {% endif %}
    ),
    transacao_deduplicada as (
        select * except (rn, priority)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_transacao
                        order by datetime_captura desc, priority
                    ) as rn
                from particoes_completas
            )
        where rn = 1
    )
select *, '{{ var("version") }}' as versao
from transacao_deduplicada
