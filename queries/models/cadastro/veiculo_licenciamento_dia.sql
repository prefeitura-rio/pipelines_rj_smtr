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

with
    licenciamento_staging as (
        select
            *,
            lag(date(data)) over (
                partition by id_veiculo, placa order by data
            ) as ultima_data,
            min(date(data)) over (partition by id_veiculo, placa) as primeira_data,
            current_date("America/Sao_Paulo") as data_processamento,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from {{ ref("licenciamento_stu_staging") }}
        where data > "2025-03-31"
    ),
    veiculo_fiscalizacao_lacre as (
        select * from {{ ref("veiculo_fiscalizacao_lacre") }}
    ),
    inicio_vinculo_preenchido as (
        select data_corrigida as data, s.* except (data)
        from
            licenciamento_staging s,
            unnest(
                generate_date_array(s.data_inicio_vinculo, data, interval 1 day)
            ) as data_corrigida
        where
            s.ultima_data is null
            and s.data != s.primeira_data
            and data_corrigida > "2025-03-31"
    ),
    dados_atuais as (
        {% if is_incremental() %}select * from {{ this }}{% else %}select {% endif %}
    ),
    veiculo_lacrado as (select distinct data, id_veiculo, placa from dados_completos)
