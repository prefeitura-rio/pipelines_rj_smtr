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

{% set staging_veiculo_fiscalizacao_lacre = ref("staging_veiculo_fiscalizacao_lacre") %}

{% if execute and is_incremental() %}
    {% set modified_partitions_query %}
        with
            datas_lacre_deslacre as (
                select distinct data_lacre_deslacre as data
                from
                    {{ staging_veiculo_fiscalizacao_lacre }},
                    unnest(
                        generate_date_array(
                            data_do_lacre,
                            coalesce(data_do_deslacre, date("{{var('date_range_end')}}")),
                            interval 1 day
                        )
                    ) as data_lacre_deslacre
                where
                    date(data) between date("{{var('date_range_start')}}") and date(
                        "{{var('date_range_end')}}"
                    )
            ),
            datas_modificadas as (
                select data
                from
                    unnest(
                        generate_date_array(
                            date("{{var('date_range_start')}}"),
                            date("{{var('date_range_end')}}")
                        ),
                        interval 1 day
                    ) as data
            ),
            datas_union as (
                select *
                from datas_lacre_deslacre
                union distinct
                select *
                from datas_union
            )
        select concat("'", data, "'") as data
        from datas_union
        where data > "2025-03-31"

    {% endset %}
    {% set modified_partitions = (
        run_query(modified_partitions_query).columns[0].values()
    ) %}

    {% set licenciamento_previous_file_query %}
        select concat("'", max(data_arquivo_fonte), "'") as data
        from {{ this }}
        where data = date_sub("{{var('date_range_start')}}", interval 1 day)

    {% endset %}

    {% set licenciamento_previous_file = (
        run_query(licenciamento_previous_file_query).columns[0].values()[0]
    ) %}

{% endif %}


with
    licenciamento_staging as (
        select
            date(data) as data,
            current_date("America/Sao_Paulo") as data_processamento,
            id_veiculo,
            placa,
            modo,
            permissao,
            ano_fabricacao,
            id_carroceria,
            id_interno_carroceria,
            carroceria,
            id_chassi,
            id_fabricante_chassi,
            nome_chassi,
            id_planta,
            tipo_veiculo,
            status,
            data_inicio_vinculo,
            data_ultima_vistoria,
            ano_ultima_vistoria,
            ultima_situacao,
            case
                when tipo_veiculo like "%BASIC%" or tipo_veiculo like "%BS%"
                then "BASICO"
                when tipo_veiculo like "%MIDI%"
                then "MIDI"
                when tipo_veiculo like "%MINI%"
                then "MINI"
                when tipo_veiculo like "%PDRON%" or tipo_veiculo like "%PADRON%"
                then "PADRON"
                when tipo_veiculo like "%ARTICULADO%"
                then "ARTICULADO"
                else safe_cast(null as string)
            end as tecnologia,
            quantidade_lotacao_pe,
            quantidade_lotacao_sentado,
            tipo_combustivel,
            indicador_ar_condicionado,
            indicador_elevador,
            indicador_usb,
            indicador_wifi,
            lag(date(data)) over (win) as ultima_data,
            min(date(data)) over (win) as primeira_data,
            date(data) as data_arquivo_fonte
        from {{ ref("staging_licenciamento_stu") }}
        window win as (partition by data, id_veiculo, placa order by data)
        where data > "2025-03-31"
    ),
    datas_faltantes as (
        select distinct
            data,
            max(l.data_arquivo_fonte) over (
                order by data rows unbounded preceding
            ) as data_arquivo_fonte
        from
            unnest(
                {% if is_incremental() %}
                    generate_date_array(
                        '{{ var("date_range_start") }}',
                        '{{ var("date_range_end") }}',
                        interval 1 day
                    )
                {% else %}
                    generate_date_array(
                        '2025-03-31', current_date("America/Sao_Paulo"), interval 1 day
                    )
                {% endif %}
            ) as data
        full outer join licenciamento_staging l using (data)
        where data > "2025-03-31"
    ),
    licenciamento_datas_preenchidas as (
        select df.data, l.* except (data)
        from licenciamento_staging l
        left join datas_faltantes df using (data_arquivo_fonte)
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
    dados_novos as (
        select *
        from inicio_vinculo_preenchido
        union all
        select *
        from licenciamento_datas_preenchidas
    ),
    veiculo_lacrado as (
        select
            dn.* except (ultima_data, primeira_data, data_arquivo_fonte),
            vfl.id_veiculo is not null as indicador_veiculo_lacrado,
            data_arquivo_fonte,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from dados_novos dn
        left join
            veiculo_fiscalizacao_lacre vfl
            on dn.id_veiculo = vfl.id_veiculo
            and dn.placa = vfl.placa
            and dn.data >= vfl.data_inicio_lacre
            and (dn.data < vfl.data_fim_lacre or vfl.data_fim_lacre is null)
    )
{% if is_incremental() %}
        ,
        dados_atuais as (select * from {{ this }}),
        dados_completos as (
            select *
            from dados_atuais

            union all

            select *
            from veiculo_lacrado
        ),
        dados_completos_sha as (
            {% set columns = (
                list_columns()
                | reject(
                    "in",
                    [
                        "data_processamento",
                        "versao",
                        "datetime_ultima_atualizacao",
                    ],
                )
                | list
            ) %}

            select
                *,
                sha256(
                    concat(
                        {% for c in columns %}
                            ifnull(cast({{ c }} as string), 'n/a')
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                ) as sha_dado
            from dados_completos
        )
    select * except (sha_dado)
    from dados_completos_sha
    window win as (partition by data, id_veiculo, placa order by data_processamento)
    qualify lag(sha_dado) over (win) != sha_dado or lag(sha_dado) over (win) is null
{% else %} select * from veiculo_lacrado
{% endif %}
