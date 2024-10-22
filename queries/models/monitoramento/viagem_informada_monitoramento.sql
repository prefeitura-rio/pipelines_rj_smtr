{{
    config(
        alias="viagem_informada",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

select
    data,
    id_viagem,
    datetime_partida,
    datetime_chegada,
    id_veiculo,
    trip_id,
    null as route_id,
    shape_id,
    servico_informado as servico,
    sentido,
    datetime_chegada as datetime_processamento,
    datetime_chegada as datetime_captura,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    {# {{ ref('viagem_completa') }} #}
    `rj-smtr.projeto_subsidio_sppo.viagem_completa`
{% if is_incremental() %}
    where data = date_sub(date('{{ var("run_date") }}'), interval 1 day)
{% endif %}
=======
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        alias="viagem_informada",
    )
}}


{% set incremental_filter %}
  DATE(data) BETWEEN DATE("{{var('date_range_start')}}") AND DATE("{{var('date_range_end')}}")
{% endset %}

{% set staging_viagem_informada_rioonibus = ref("staging_viagem_informada_rioonibus") %}
{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
      SELECT DISTINCT
        CONCAT("'", DATE(data_viagem), "'") AS data_viagem
      FROM
        {{ staging_viagem_informada_rioonibus }}
      WHERE
        {{ incremental_filter }}
        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    staging_rioonibus as (
        select
            data_viagem as data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            case
                when sentido = 'I'
                then 'Ida'
                when sentido = 'V'
                then 'Volta'
                when sentido = 'C'
                then 'Circular'
                else sentido
            end as sentido,
            fornecedor as fonte_gps,
            datetime_processamento,
            timestamp_captura as datetime_captura,
        from {{ staging_viagem_informada_rioonibus }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    complete_partitions as (
        select *, 0 as priority
        from staging_rioonibus

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select * except (versao, datetime_ultima_atualizacao), 1 as priority
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    deduplicado as (
        select * except (rn, priority)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_viagem order by datetime_captura desc, priority
                    ) as rn
                from complete_partitions
            )
        where rn = 1
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from deduplicado