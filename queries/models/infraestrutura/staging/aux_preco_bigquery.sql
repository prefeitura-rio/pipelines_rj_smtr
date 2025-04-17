{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    valores_reais as (
        select
            date(export_time) as data,
            list_price.tiered_rates[
                offset(1)
            ].account_currency_amount as valor_tib_real,
            list_price.tiered_rates[offset(1)].usd_amount as valor_tib_dolar,
            currency_conversion_rate as taxa_conversao_real,
            'Real' as origem
        from
            {{
                source(
                    "cloudcosts",
                    "cloud_pricing_export",
                )
            }}
        where
            {% if is_incremental() %}
                date(
                    _partitiontime,
                    'America/Sao_Paulo'
                ) between date('{{ var("date_range_start") }}') and date(
                    '{{ var("date_range_end") }}'
                )
                and
            {% endif %}
            service.description like '%BigQuery%'
            and pricing_unit like '%TEBIBYTE%'
            and sku.description = 'Analysis (us-central1)'
    ),
    valores_fixos_anteriores as (
        select
            data,
            41.275113718 as valor_tib_real,
            6.25 as valor_tib_dolar,
            0.151422962 as taxa_conversao_real,
            'Fixado Manualmente' as origem
        from
            unnest(
                generate_date_array(
                    date('{{ var("data_inicial_custo_cloud") }}'),
                    date('{{ var("data_final_valor_bq_manual") }}'),
                    interval 1 day
                )
            ) as data
    ),
    valores_union as (
        select *
        from valores_fixos_anteriores
        union all
        select *
        from valores_reais
    ),
    data_preenchida as (
        select * except (priority)
        from
            (
                select *, 0 as priority
                from valores_union

                union all

                select
                    {% if is_incremental() %} date('{{ var("date_range_end") }}')
                    {% else %} current_date('America/Sao_Paulo')
                    {% endif %} as data,
                    * except (data, origem),
                    'Preenchido' as origem,
                    1 as priority
                from valores_union
                qualify data = max(data) over ()
            )
        qualify row_number() over (partition by data order by priority) = 1
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime('America/Sao_Paulo') as datetime_ultima_atualizacao
from data_preenchida
