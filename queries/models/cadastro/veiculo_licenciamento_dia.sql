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
        select concat("'", ifnull(max(data_arquivo_fonte), date("{{ var('date_range_start') }}")), "'") as data
        from {{ this }}
        where data = date_sub(date("{{ var('date_range_start') }}"), interval 1 day)

    {% endset %}

    {% set licenciamento_previous_file = (
        run_query(licenciamento_previous_file_query).columns[0].values()[0]
    ) %}

    {% set inicio_vinculo_partitions_query %}
        with staging as (
            select date(data) as data, id_veiculo, placa, data_inicio_vinculo
            from {{ staging_licenciamento_stu }}
            where
                date(data) between date({{ licenciamento_previous_file }}) and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        novos_veiculos as (
            select *
            from staging
            qualify lag(data_inicio_vinculo) over(partition by id_veiculo, placa order by data) is null
        ),
        menor_inicio_vinculo as (
            select
                min(data_inicio_vinculo) as data_inicio_vinculo
            from novos_veiculos
            where data != date({{ licenciamento_previous_file }})
        )
        select distinct concat("'", data, "'") as data
        from menor_inicio_vinculo,
        unnest(generate_date_array(data_inicio_vinculo, date("{{ var('date_range_start') }}"), interval 1 day)) as data

    {% endset %}

    {% set inicio_vinculo_partitions = (
        run_query(inicio_vinculo_partitions_query).columns[0].values()
    ) %}

    {% set vistoria_partitions_query %}
        with staging as (
            select date(data) as data, id_veiculo, placa, data_ultima_vistoria
            from {{ staging_licenciamento_stu }}
            where
                date(data) between date({{ licenciamento_previous_file }}) and date(
                    "{{ var('date_range_end') }}"
                )
        ),
        veiculos_vistoriados as (
            select *
            from staging
            qualify
                (lag(data_ultima_vistoria) over(win) is null and data_ultima_vistoria is not null)
                or lag(data_ultima_vistoria) over(win) != data_ultima_vistoria
            window win as (partition by id_veiculo, placa order by data)
        ),
        menor_data_vistoria as (
            select
                min(data_ultima_vistoria) as data_ultima_vistoria
            from veiculos_vistoriados
            where data != date({{ licenciamento_previous_file }})
        )
        select distinct concat("'", data, "'") as data
        from menor_data_vistoria,
        unnest(generate_date_array(data_ultima_vistoria, date("{{ var('date_range_start') }}"), interval 1 day)) as data

    {% endset %}
    {% set vistoria_partitions = (
        run_query(vistoria_partitions_query).columns[0].values()
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
        and data > '{{ var("data_final_veiculo_arquitetura_1") }}'

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
        where
            data > '{{ var("data_final_veiculo_arquitetura_1") }}'
            {% if is_incremental() %}
                and date(data) between date({{ licenciamento_previous_file }}) and date(
                    "{{ var('date_range_end') }}"
                )
            {% endif %}
        window win as (partition by data, id_veiculo, placa order by data)
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
                        date('{{ var("date_range_start") }}'),
                        date('{{ var("date_range_end") }}'),
                        interval 1 day
                    )
                {% else %}
                    generate_date_array(
                        '{{ var("data_final_veiculo_arquitetura_1") }}',
                        current_date("America/Sao_Paulo"),
                        interval 1 day
                    )
                {% endif %}
            ) as data
        full outer join licenciamento_staging l using (data)
        where data > '{{ var("data_final_veiculo_arquitetura_1") }}'
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
            and data_corrigida > '{{ var("data_final_veiculo_arquitetura_1") }}'
    ),
    dados_novos as (
        select *
        from inicio_vinculo_preenchido
        union all
        select *
        from licenciamento_datas_preenchidas
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
                {% if vistoria_partitions | length > 0 %}
                    or data in ({{ vistoria_partitions | join(", ") }})

                {% endif %}
        ),
        dados_novos_lacre_vistoria as (
            select * except (ultima_data, primeira_data)
            from dados_novos

            {% if lacre_partitions | length > 0 or vistoria_partitions | length > 0 %}
                union all

                select
                    da.data,
                    current_date("America/Sao_Paulo") as data_processamento,
                    da.* except (
                        data,
                        data_processamento,
                        indicador_veiculo_lacrado,
                        versao,
                        datetime_ultima_atualizacao,
                        id_execucao_dbt
                    )
                from dados_atuais da
                left join
                    (
                        select distinct data, id_veiculo, placa from dados_novos
                    ) dn using (data, id_veiculo, placa)
                where
                    (
                        {% if lacre_partitions | length > 0 %}
                            data in ({{ lacre_partitions | join(", ") }})
                        {% else %}data = '2000-01-01'
                        {% endif %}
                        or {% if vistoria_partitions | length > 0 %}
                            data in ({{ vistoria_partitions | join(", ") }})
                        {% else %} data = '2000-01-01'
                        {% endif %}
                    )
                    and dn.data is null
                qualify
                    row_number() over (
                        partition by data, id_veiculo, placa
                        order by data_processamento desc
                    )
                    = 1

            {% endif %}
        )
    {% else %} dados_novos_lacre_vistoria as (select * from dados_novos)
    {% endif %},
    veiculo_lacrado as (
        select
            dn.*,
            vfl.id_veiculo is not null as indicador_veiculo_lacrado,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from dados_novos_lacre_vistoria dn
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
    ),
    data_vistoria_atualizacao as (
        select
            date(data) as data,
            id_veiculo,
            placa,
            data_ultima_vistoria,
            ano_ultima_vistoria
        from dados_novos
        qualify
            (
                lag(data_ultima_vistoria) over (win) is null
                and data_ultima_vistoria is not null
            )
            or lag(data_ultima_vistoria) over (win) != data_ultima_vistoria
        window win as (partition by id_veiculo, placa order by data)

    ),
    nova_data_ultima_vistoria as (
        select
            nova_data as data,
            id_veiculo,
            placa,
            data_ultima_vistoria,
            ano_ultima_vistoria
        from
            data_vistoria_atualizacao d,
            unnest(
                generate_date_array(data_ultima_vistoria, data, interval 1 day)
            ) as nova_data
        qualify
            row_number() over (
                partition by nova_data, id_veiculo, placa order by d.data desc
            )
            = 1
    ),
    veiculo_vistoriado as (
        select
            v.* except (data_ultima_vistoria, ano_ultima_vistoria),
            ifnull(
                d.data_ultima_vistoria, v.data_ultima_vistoria
            ) as data_ultima_vistoria,
            ifnull(d.ano_ultima_vistoria, v.ano_ultima_vistoria) as ano_ultima_vistoria
        from veiculo_lacrado_deduplicado v
        left join nova_data_ultima_vistoria d using (data, id_veiculo, placa)
    ),
    final as (
        {% if is_incremental() %}

            with
                dados_completos as (
                    select *, 1 as priority
                    from dados_atuais

                    union all by name

                    select *, cast(null as string) as id_execucao_dbt, 0 as priority
                    from veiculo_vistoriado
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
                                "id_execucao_dbt",
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
                ),
                dados_completos_invocation_id as (
                    select
                        * except (id_execucao_dbt),
                        case
                            when
                                lag(sha_dado) over (win) != sha_dado
                                or (
                                    lag(sha_dado) over (win) is null
                                    and count(*) over (win) = 1
                                )
                            then '{{ invocation_id }}'
                            else
                                ifnull(id_execucao_dbt, lag(id_execucao_dbt) over (win))
                        end as id_execucao_dbt
                    from dados_completos_sha
                    window
                        win as (
                            partition by data, id_veiculo, placa, data_processamento
                            order by priority desc
                        )

                ),
                dados_completos_deduplicados as (
                    select *
                    from dados_completos_invocation_id
                    qualify
                        row_number() over (
                            partition by data, data_processamento, id_veiculo, placa
                            order by priority
                        )
                        = 1
                )
            select * except (sha_dado)
            from dados_completos_deduplicados
            qualify
                lag(sha_dado) over (win) != sha_dado or lag(sha_dado) over (win) is null
            window
                win as (
                    partition by data, id_veiculo, placa order by data_processamento
                )
        {% else %}
            select *, '{{ invocation_id }}' as id_execucao_dbt from veiculo_vistoriado
        {% endif %}
    )
select
    data,
    data_processamento,
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
    tecnologia,
    quantidade_lotacao_pe,
    quantidade_lotacao_sentado,
    tipo_combustivel,
    indicador_ar_condicionado,
    indicador_elevador,
    indicador_usb,
    indicador_wifi,
    indicador_veiculo_lacrado,
    data_arquivo_fonte,
    versao,
    datetime_ultima_atualizacao,
    id_execucao_dbt
from final
where data <= data_processamento
