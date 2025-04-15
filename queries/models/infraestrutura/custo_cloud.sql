{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
  date(_PARTITIONTIME) between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% set billing_staging = source(
    "cloudcosts",
    "gcp_billing_export_resource_v1_010693_B1EC8D_C0D4DF",
) %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select distinct concat("'", date(usage_start_time), "'") as data
            from {{ billing_staging }},
            where
                {{ incremental_filter }}

        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% endif %}
{% endif %}

with
    billing as (
        select *
        from {{ billing_staging }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    )
select
    date(usage_start_time) as data,
    service.description as tipo_servico,
    sku.description as sku,
    project.name as projeto,
    cost as custo,
    currency_conversion_rate as taxa_conversao_real,
    usage.amount as quantidade_uso,
    usage.unit as unidade_uso,
    usage.amount_in_pricing_units as quantidade_unidade_preco,
    usage.pricing_unit as unidade_preco,
from billing
where usage_start_time >= "2024-10-01"
