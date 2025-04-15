{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
with
    prod as (
        select
            date(timestamp, "America/Sao_Paulo") as data,
            resource.labels.project_id as projeto,
            protopayload_auditlog.authenticationinfo.principalemail as usuario,
            protopayload_auditlog.methodname as metodo,
            protopayload_auditlog.resourcename as id_job,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"flow_name"\s*:\s*"([^"]+)"'
            ) as nome_flow,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"dashboard_name"\s*:\s*"([^"]+)"'
            ) as nome_dashboard,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query
            as query,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalprocessedbytes
            as bytes_processados,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
            as bytes_faturados,
            (
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
                / pow(1024, 4)
            ) as tib_processado

        from {{ source("bq_logs_prod", "cloudaudit_googleapis_com_data_access_*") }}
        where
            {% if is_incremental() %}
                parse_date('%Y%m%d', _table_suffix) between date_sub(
                    date("{{var('date_range_start')}}"), interval 1 day
                ) and date("{{var('date_range_end')}}")
                and date(
                    timestamp,
                    "America/Sao_Paulo"
                ) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and
            {% endif %}
            _table_suffix >= "20240930"
            and date(timestamp, "America/Sao_Paulo") >= "2024-10-01"
    ),
    dev as (
        select
            date(timestamp, "America/Sao_Paulo") as data,
            resource.labels.project_id as projeto,
            protopayload_auditlog.authenticationinfo.principalemail as usuario,
            protopayload_auditlog.methodname as metodo,
            protopayload_auditlog.resourcename as id_job,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"flow_name"\s*:\s*"([^"]+)"'
            ) as nome_flow,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"dashboard_name"\s*:\s*"([^"]+)"'
            ) as nome_dashboard,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query
            as query,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalprocessedbytes
            as bytes_processados,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
            as bytes_faturados,
            (
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
                / pow(1024, 4)
            ) as tib_processado

        from {{ source("bq_logs_dev", "cloudaudit_googleapis_com_data_access") }}
        where
            {% if is_incremental() %}
                date(timestamp, 'America/Sao_Paulo') between date_sub(
                    date("{{var('date_range_start')}}"), interval 1 day
                ) and date("{{var('date_range_end')}}")
                and date(
                    timestamp,
                    "America/Sao_Paulo"
                ) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and
            {% endif %}
            and date(timestamp, "America/Sao_Paulo") >= "2024-10-01"
    ),
    staging as (
        select
            date(timestamp, "America/Sao_Paulo") as data,
            resource.labels.project_id as projeto,
            protopayload_auditlog.authenticationinfo.principalemail as usuario,
            protopayload_auditlog.methodname as metodo,
            protopayload_auditlog.resourcename as id_job,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"flow_name"\s*:\s*"([^"]+)"'
            ) as nome_flow,
            regexp_extract(
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query,
                r'"dashboard_name"\s*:\s*"([^"]+)"'
            ) as nome_dashboard,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobconfiguration.query.query
            as query,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalprocessedbytes
            as bytes_processados,
            protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
            as bytes_faturados,
            (
                protopayload_auditlog.servicedata_v1_bigquery.jobcompletedevent.job.jobstatistics.totalbilledbytes
                / pow(1024, 4)
            ) as tib_processado

        from {{ source("bq_logs_staging", "cloudaudit_googleapis_com_data_access") }}
        where
            {% if is_incremental() %}
                date(timestamp, 'America/Sao_Paulo') between date_sub(
                    date("{{var('date_range_start')}}"), interval 1 day
                ) and date("{{var('date_range_end')}}")
                and date(
                    timestamp,
                    "America/Sao_Paulo"
                ) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and
            {% endif %}
            and date(timestamp, "America/Sao_Paulo") >= "2024-10-01"
    ),
    union_projetos as (
        select *
        from prod

        union all

        select *
        from dev

        union all

        select *
        from staging
    )
select
    data,
    projeto,
    usuario,
    metodo,
    id_job,
    query,
    coalesce(nome_flow, nome_dashboard) as processo_execucao,
    case
        when nome_flow is not null
        then "Flow"
        when nome_dashboard is not null
        then "Dashboard" bytes_processados, bytes_faturados, tib_processado
        else "Outro"
    end as tipo_processo_execucao
from union_projetos
where usuario is not null and bytes_faturados > 0
