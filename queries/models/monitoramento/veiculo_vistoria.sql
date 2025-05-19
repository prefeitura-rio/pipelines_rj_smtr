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

{% set incremental_filter %}
    data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
{% endset %}

{% set aux_licenciamento = ref("aux_licenciamento") %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select concat("'", date(data_ultima_vistoria), "'") AS data
            from {{ aux_licenciamento }}
            where data_ultima_vistoria is not null and {{ incremental_filter }}

        {% endset %}

        {% set partitions = run_query(transacao_partitions_query).columns[0].values() %}

    {% endif %}
{% endif %}

with
    licenciamento as (
        select * from {{ aux_licenciamento }} where {{ incremental_filter }}
    ),
    novos_dados as (
        select
            data_ultima_vistoria as data,
            id_veiculo,
            placa,
            ano_ultima_vistoria as ano_vistoria,
            data as data_inclusao
        from licenciamento
        where data_ultima_vistoria is not null
        qualify
            data_ultima_vistoria != lag(data_ultima_vistoria) over (
                partition by id_veiculo, placa order by data
            )
            or lag(data_ultima_vistoria) over (
                partition by id_veiculo, placa order by data
            )
            is null
    ),
    {% set columns = (
        list_columns()
        | reject(
            "in",
            [
                "versao",
                "datetime_ultima_atualizacao",
                "sha_dado",
            ],
        )
        | list
    ) %}

    {% set sha_column %}
            sha256(
                concat(
                    {% for c in columns %}
                        ifnull(cast({{ c }} as string), 'n/a')
                        {% if not loop.last %}, {% endif %}
                    {% endfor %}
                )
                )
    {% endset %}
    novos_dados_sha as (select *, {{ sha_column }} as sha_dado from novos_dados)
{% if is_incremental() %}
        ,
        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} data = "2000-01-01"
                {% endif %}
        ),
        dados_consolidados as (
            select
                data,
                id_veiculo,
                placa,
                coalesce(n.ano_vistoria, a.ano_vistoria) as ano_vistoria,
                case
                    when n.data_inclusao < a.data_inclusao or a.data_inclusao is null
                    then n.data_inclusao
                    else a.data_inclusao
                end as data_inclusao,
                a.sha_dado as sha_dado_atual,
                n.sha_dado as sha_dado_novo,
                a.datetime_ultima_atualizacao
            from dados_atuais a
            full outer join novos_dados_sha n using (data, id_veiculo, placa)
        )
    select
        * except (sha_dado_atual, sha_dado_novo),
        coalesce(sha_dado_novo, sha_dado_atual) as sha_dado,
        case
            when sha_dado_atual != sha_dado_novo or sha_dado_atual is null
            then current_datetime("America/Sao_Paulo")
            else datetime_ultima_atualizacao
        end as datetime_ultima_atualizacao,
        '{{ var("version") }}' as versao
    from dados_consolidados

{% else %}
    select
        *,
        current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
        '{{ var("version") }}' as versao
    from novos_dados_sha

{% endif %}
