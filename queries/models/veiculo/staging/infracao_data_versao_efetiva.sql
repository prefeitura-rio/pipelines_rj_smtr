{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
with
    infracao_date as (
        select distinct date(data) as data_infracao
        from  {{ ref("infracao_staging") }}
            -- `rj-smtr.veiculo_staging.infracao`
        {% if is_incremental() %}
            where
                data
                between "{{ var('start_date')}}"
                and "{{ modules.datetime.date.fromisoformat(var('end_date')) + modules.datetime.timedelta(14) }}"
        {% endif %}
    ),
    periodo as (
        select *
        from
            unnest(
                -- Primeira data de captura de infração 
                generate_date_array('2023-02-10', current_date("America/Sao_Paulo"))
            ) as data
        {% if is_incremental() %}
            where data between "{{ var('start_date')}}" and "{{ var('end_date')}}"
        {% endif %}
    ),
    data_versao_calc as (
        select
            periodo.data,
            (
                select min(data_infracao)
                from infracao_date
                where data_infracao >= date_add(periodo.data, interval 7 day)
            ) as data_versao
        from periodo
    )
select *
from data_versao_calc
where data_versao is not null
