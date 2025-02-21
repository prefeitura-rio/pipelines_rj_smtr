{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_auto_infracao"],
        incremental_strategy="insert_overwrite",
    )
}}

{%- if execute and is_incremental() %}
  {% set infracao_date = run_query(get_violation_date()).columns[0].values()[0] %}
{% endif -%}

with
    infracao as (
        select * except (data), safe_cast(data as date) as data
        from {{ ref("infracao_staging") }} as t
        {% if is_incremental() %}
            where
                date(data) = date("{{ infracao_date }}")
        {% endif %}
    ),
    infracao_rn as (
        select
            *,
            row_number() over (
                partition by data, id_auto_infracao order by timestamp_captura desc
            ) rn
        from infracao
    )
select
    * except (rn),
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao
from infracao_rn
where rn = 1
