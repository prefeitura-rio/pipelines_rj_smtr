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

{% set staging_licenciamento_stu = ref("staging_licenciamento_stu") %}
{% set staging_veiculo_fiscalizacao_lacre = ref("staging_veiculo_fiscalizacao_lacre") %}

{% if execute and is_incremental() %}
    {% set licenciamento_previous_file_query %}
        select concat("'", max(data_arquivo_fonte), "'") as data
        from {{ this }}
        where data = date_sub("{{ var('date_range_start') }}", interval 1 day)

    {% endset %}

    {% set licenciamento_previous_file = (
        run_query(licenciamento_previous_file_query).columns[0].values()[0]
    ) %}

    {% set inicio_vinculo_partitions_query %}
        with novos_veiculos as (
            select date(data) as data, data_inicio_vinculo
            from {{ staging_licenciamento_stu }}
            where
                date(data) between date("{{ licenciamento_previous_file }}") and date(
                    "{{ var('date_range_end') }}"
                )
            qualify lag(data_inicio_vinculo) over(partition by id_veiculo, placa order by data) is null
        ),
        menor_inicio_vinculo as (
            select
                min(data_inicio_vinculo) as data_inicio_vinculo
            from novos_veiculos
            where data != date("{{ licenciamento_previous_file }}")
        )
        select distinct data
        from menor_inicio_vinculo,
        unnest(generate_date_array(data_inicio_vinculo, date("{{ var('date_range_start') }}"), interval 1 day)) as data

    {% endset %}
    {% set inicio_vinculo_partitions = (
        run_query(inicio_vinculo_partitions_query).columns[0].values()
    ) %}

    {% set lacre_partitions_query %}
        select distinct concat("'", data_lacre_deslacre, "'") as data
        from
            {{ staging_veiculo_fiscalizacao_lacre }},
            unnest(
                generate_date_array(
                    data_do_lacre,
                    coalesce(data_do_deslacre, date("{{ var('date_range_end') }}")),
                    interval 1 day
                )
            ) as data_lacre_deslacre
        where
            date(data) between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
        and data > "2025-03-31"

    {% endset %}
    {% set lacre_partitions = run_query(lacre_partitions_query).columns[0].values() %}

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
        where
            data > "2025-03-31"
            {% if is_incremental() %}
                and date(
                    data
                ) between date("{{ licenciamento_previous_file }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% endif %}
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
    {% if is_incremental() %}
        dados_atuais as (
            select *
            from {{ this }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
                {% if lacre_partitions | length > 0 %}
                    or data in ({{ lacre_partitions | join(", ") }})

                {% endif %}
                {% if inicio_vinculo_partitions | length > 0 %}
                    or data in ({{ inicio_vinculo_partitions | join(", ") }})

                {% endif %}
        ),
        dados_novos as (
            select *
            from inicio_vinculo_preenchido
            union all
            select *
            from licenciamento_datas_preenchidas
            union all
            {% if lacre_partitions | length > 0 %}
                select
                    data,
                    current_date("America/Sao_Paulo") as data_processamento,
                    * except (
                        data,
                        data_processamento,
                        indicador_veiculo_lacrado,
                        versao,
                        datetime_ultima_atualizacao
                    )
                from dados_atuais
                where data in ({{ lacre_partitions | join(", ") }})
                qualify
                    row_number() over (
                        partition by data, id_veiculo, placa
                        order by data_processamento desc
                    )
                    = 1
            {% endif %}

        )
    {% else %}
        dados_novos as (
            select *
            from inicio_vinculo_preenchido
            union all
            select *
            from licenciamento_datas_preenchidas
        )
    {% endif %},
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
    ),
    veiculo_lacrado_deduplicado as (
        select *
        from veiculo_lacrado
        qualify
            row_number() over (
                partition by data, id_veiculo, placa
                order by indicador_veiculo_lacrado desc
            )
            = 1
    )
{% if is_incremental() %}
        ,
        dados_completos as (
            select *
            from dados_atuais

            union all

            select *
            from veiculo_lacrado_deduplicado
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
{% else %} select * from veiculo_lacrado_deduplicado
{% endif %}
