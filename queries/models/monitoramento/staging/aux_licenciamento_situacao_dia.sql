{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% if execute and is_incremental() %}
    {% set last_partition_query %}
        select concat("'", max(parse_date("%Y%m%d", partition_id)), "'") as data
        from `{{ this.database }}.{{ this.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ this.identifier }}"
            and partition_id != "__NULL__"
            and parse_date("%Y%m%d", partition_id) < datetime("{{ var('date_range_start') }}")

    {% endset %}

    {% set last_partition = run_query(last_partition_query).columns[0].values() %}

    {% if last_partition | length > 0 %} {% set last_partition = last_partition[0] %}

    {% else %} {% set last_partition = "" %}

    {% endif %}

{% endif %}

with
    novos_dados as (
        select data, id_veiculo, placa, ultima_situacao as situacao, 'staging' as origem
        from {{ ref("aux_licenciamento") }}
        {% if is_incremental() %}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

        {% endif %}
    ),
    dados_completos as (
        {% if is_incremental() and last_partition != "" %}
            select data, id_veiculo, placa, situacao, 'tratada' as origem
            from {{ this }}
            where data = {{ last_partition }}

            union all

        {% endif %}

        select *
        from novos_dados
    )
select
    data,
    id_veiculo,
    placa,
    situacao,
    lag(situacao) over (
        partition by id_veiculo, placa order by data
    ) as situacao_anterior
from dados_completos
where origem = 'staging'
