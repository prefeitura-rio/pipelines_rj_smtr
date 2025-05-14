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
        from {{ ref("licenciamento") }}
        {% if is_incremental() %}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        {% endif %}
    ),
    datas as (
        select data
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
    ),
    novos_dados as (
        select
            ifnull(
                min(data_inicio_vinculo), min(data_arquivo_fonte)
            ) as data_inicio_vinculo,
            case
                when max(data) != (select max(data) from licenciamento) then max(data)
            end as data_fim_vinculo,
            id_veiculo,
            placa,
            max(data_ultima_vistoria) as data_ultima_vistoria,
            max_by(status, data) as status,
            min(data_arquivo_fonte) as data_inclusao,
            max(data_arquivo_fonte) as data_ultimo_arquivo
        from licenciamento_datas_preenchidas
        group by id_veiculo, placa
    ),
    sha_dados as (
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
        dados_atuais as (select * from {{ this }}),
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
                case
                    when a.data_ultima_vistoria is null
                    then n.data_ultima_vistoria
                    when a.data_ultima_vistoria > n.data_ultima_vistoria
                    then a.data_ultima_vistoria
                    else n.data_ultima_vistoria
                end as data_ultima_vistoria,
                case
                    when a.data_ultimo_arquivo > n.data_ultimo_arquivo
                    then a.status
                    else n.status
                end as status,
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
                sha_dado
            from dados_atuais a
            full outer join sha_dados n using (id_veiculo, placa)
        ),
        sha_dados_consolidados as (
            select *, {{ sha_column }} as sha_dado from dados_consolidados
        )
    select
        c.*,
        case
            when a.sha_dado is null or a.sha_dado != c.sha_dado
            then current_datetime("America/Sao_Paulo")
            else datetime_ultima_atualizacao
        end as datetime_ultima_atualizacao,
        '{{ var("version") }}' as versao
    from sha_dados_consolidados c
    left join dados_atuais a using (id_veiculo, placa)
{% else %}
    select
        *,
        current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
        '{{ var("version") }}' as versao
    from novos_dados
{% endif %}
