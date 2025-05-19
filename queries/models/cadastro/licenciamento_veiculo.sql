{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_inicio_vinculo",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

with
    licenciamento as (
        select *
        from {{ ref("aux_licenciamento") }}
        {% if is_incremental() %}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        {% endif %}
    ),
    {# datas as ( #}
    {# select data
        from
            unnest(
                generate_date_array(
                    (select min(data) from licenciamento),
                    (select max(data) from licenciamento)
                )
            ) as data
    ),
    datas_sem_dados as (
        select * from datas where data not in (select distinct data from licenciamento)
    ),
    datas_preenchimento as (
        select
            data,
            last_value(ultima_data ignore nulls) over (
                order by data rows between unbounded preceding and current row
            ) as data_ultimo_arquivo
        from
            (
                select data, null as data_ultimo_arquivo
                from datas_sem_dados

                union all

                select distinct data, data as data_ultimo_arquivo
                from licenciamento
            )
    ),
    licenciamento_datas_preenchidas as (
        select l.*, d.data_ultimo_arquivo as data_arquivo_fonte
        from datas_preenchimento d
        join licenciamento l on d.data_ultimo_arquivo = l.data
    ), #}
    {% set constant_columns = [
        "modo",
        "permissao",
        "tecnologia",
        "ano_fabricacao",
        "id_carroceria",
        "id_interno_carroceria",
        "carroceria",
        "id_chassi",
        "id_fabricante_chassi",
        "nome_chassi",
        "id_planta",
        "tipo_combustivel",
        "tipo_veiculo",
        "quantidade_lotacao_pe",
        "quantidade_lotacao_sentado",
        "indicador_ar_condicionado",
        "indicador_elevador",
        "indicador_usb",
        "indicador_wifi",
    ] %}
    novos_dados as (
        select
            ifnull(min(data_inicio_vinculo), min(data)) as data_inicio_vinculo,
            case
                when max(data) != (select max(data) from licenciamento) then max(data)
            end as data_fim_vinculo,
            id_veiculo,
            placa,
            {% for col in constant_columns %} {{ col }}, {% endfor %}
            min(data) as data_inclusao,
            max(data) as data_ultimo_arquivo
        from licenciamento
        group by id_veiculo, placa
    ),
    novos_dados_sha as (
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

        select *, {{ sha_column }} as sha_dado
        from novos_dados
    )
{% if is_incremental() %}
        ,
        dados_consolidados as (
            select
                case
                    when o.data_inicio_vinculo is null
                    then n.data_inicio_vinculo
                    when o.data_inicio_vinculo < n.data_inicio_vinculo
                    then o.data_inicio_vinculo
                    else n.data_inicio_vinculo
                end as data_inicio_vinculo,
                case
                    when o.data_fim_vinculo is null
                    then n.data_fim_vinculo
                    when o.data_fim_vinculo > n.data_fim_vinculo
                    then o.data_fim_vinculo
                    else n.data_fim_vinculo
                end as data_fim_vinculo,
                id_veiculo,
                placa,
                {% for col in constant_columns %}
                    case
                        when a.id_veiculo is not null then a.{{ col }} else n.{{ col }}
                    end as {{ col }},
                {% endfor %}
                case
                    when a.data_inclusao is null
                    then n.data_inclusao
                    when a.data_inclusao < n.data_inclusao
                    then a.data_inclusao
                    else n.data_inclusao
                end as data_inclusao,
                case
                    when a.data_ultimo_arquivo is null
                    then n.data_ultimo_arquivo
                    when a.data_ultimo_arquivo > n.data_ultimo_arquivo
                    then a.data_ultimo_arquivo
                    else n.data_ultimo_arquivo
                end as data_ultimo_arquivo,
                a.sha_dado as sha_dado_atual,
                n.sha_dado as sha_dado_novo
            from {{ this }} a
            full outer join novos_dados_sha n using (id_veiculo, placa)
        ),
        sha_dados_consolidados as (
            select *, {{ sha_column }} as sha_dado from dados_consolidados
        )
    select
        c.* except (sha_dado_atual, sha_dado_novo),
        coalesce(sha_dado_novo, sha_dado_atual) as sha_dado,
        case
            when sha_dado_atual null or sha_dado_atual != sha_dado_novo
            then current_datetime("America/Sao_Paulo")
            else datetime_ultima_atualizacao
        end as datetime_ultima_atualizacao,
        '{{ var("version") }}' as versao
    from sha_dados_consolidados
{% else %}
    select
        *,
        current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
        '{{ var("version") }}' as versao
    from novos_dados
{% endif %}
