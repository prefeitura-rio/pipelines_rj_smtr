-- depends_on: {{ ref('infracao_data_versao_efetiva') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% if is_incremental() and execute %}
    {% set infracao_dates = run_query(get_violation_dates()) %}
    {% set min_infracao_date = infracao_dates.columns[0].values()[0]%}
    {% set max_infracao_date = infracao_dates.columns[1].values()[0]%}
{% endif %}
with
    infracao as (
        select * except (data), date(data) as data
        from {{ ref("infracao_staging") }} as i
        {% if is_incremental() %}
            where date(data) between date("{{ min_infracao_date }}") and date("{{ max_infracao_date }}")
        {% endif %}
    )
select
    *,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao
from infracao
qualify
    row_number() over (
        partition by data, id_auto_infracao order by timestamp_captura desc
    )
    = 1
