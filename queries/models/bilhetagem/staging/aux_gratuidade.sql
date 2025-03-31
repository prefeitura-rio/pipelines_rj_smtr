{{
    config(
        materialized="table",
        incremental_strategy="merge",
        unique_key="id_gratuidade",
        partition_by={
            "field": "id_cliente",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000, "interval": 10000},
        },
    )
}}


{% set staging_gratuidade = ref("staging_gratuidade") %}


with
    gratuidade_complete_partitions as (
        select
            cast(cast(cd_cliente as float64) as int64) as id_cliente,
            id as id_gratuidade,
            tipo_gratuidade,
            deficiencia_permanente,
            rede_ensino,
            data_inclusao as data_inicio_validade,
            timestamp_captura
        from {{ staging_gratuidade }}
        {% if is_incremental() %}
            where
                data between date('{{var("date_range_start")}}') and date(
                    '{{var("date_range_end")}}'
                )
        {% endif %}
    ),
    gratuidade_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_gratuidade order by timestamp_captura desc
                    ) as rn
                from gratuidade_complete_partitions
            )
        where rn = 1
    )
select
    id_cliente,
    id_gratuidade,
    tipo_gratuidade,
    deficiencia_permanente,
    rede_ensino,
    data_inicio_validade,
    lead(data_inicio_validade) over (
        partition by id_cliente order by data_inicio_validade
    ) as data_fim_validade,
    timestamp_captura
from gratuidade_deduplicada
