{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_auto_infracao"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    infracao as (
        select i.* except (data), safe_cast(i.data as date) as data
        from {{ ref("infracao_staging") }} as i
        left join {{ ref("infracao_data_versao_efetiva") }} dve
        on i.data = dve.data_versao
        {% if is_incremental() %}
            where dve.data between date("{{ var('run_date') }}") and date("{{ var('run_date') }}")
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
