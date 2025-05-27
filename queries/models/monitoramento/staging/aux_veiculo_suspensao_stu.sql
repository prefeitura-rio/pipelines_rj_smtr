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
    licenciamento_situacao_dia as (
        select
            data,
            id_veiculo,
            placa,
            ifnull(situacao, 'Normal') as situacao,
            ifnull(situacao_anterior, 'Normal') as situacao_anterior,
            data as data_inclusao,
            'stu' as origem
        from {{ ref("aux_licenciamento_situacao_dia") }}
        where
            ifnull(situacao_anterior, '') != ifnull(situacao, '')
            and (situacao = 'Suspenso' or situacao_anterior = 'Suspenso')
            {% if is_incremental() %}
                and data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% endif %}
    ),
    dados_atuais as (
        {% if is_incremental() %}
            select
                data_inicio_suspensao,
                data_fim_suspensao,
                id_veiculo,
                placa,
                situacao,
                situacao_anterior,
                origem,
                data_inclusao_inicio,
                data_inclusao_fim,
                origem_data_inicio,
                origem_data_fim
            from {{ this }}

        {% else %}
            select
                cast(null as date) as data_inicio_suspensao,
                cast(null as date) as data_fim_suspensao,
                cast(null as string) as id_veiculo,
                cast(null as string) as placa,
                cast(null as string) as situacao,
                cast(null as string) as situacao_anterior,
                cast(null as string) as origem,
                cast(null as date) as data_inclusao_inicio,
                cast(null as date) as data_inclusao_fim,
                cast(null as string) as origem_data_inicio,
                cast(null as string) as origem_data_fim

        {% endif %}
    ),
    dados_atuais_formatados as (
        select
            data_inicio_suspensao as data,
            id_veiculo,
            placa,
            'Suspenso' as situacao,
            'Normal' as situacao_anterior,
            data_inclusao_inicio as data_inclusao,
            origem_data_inicio as origem,
            1 as priority
        from dados_atuais
        where data_inicio_suspensao is not null

        union all

        select
            data_fim_suspensao as data,
            id_veiculo,
            placa,
            'Normal' as situacao,
            'Suspenso' as situacao_anterior,
            data_inclusao_fim as data_inclusao,
            origem_data_fim as origem,
            1 as priority
        from dados_atuais
        where data_fim_suspensao is not null
    ),
    stu as (
        select * 1 as priority
        from dados_atuais_formatados
        where origem = 'stu'

        union all

        select *, 0 as priority
        from licenciamento_situacao_dia
    ),

    stu_deduplicado as datas_completas as (
        {% if is_incremental() %}
            select
                data_inicio_suspensao as data,
                id_veiculo,
                placa,
                'Suspenso' as situacao,
                'Normal' as situacao_anterior,
                origem_data_inicio as origem
                1 as priority
            from {{ this }}

            union all

            select
                data_fim_suspensao as data,
                id_veiculo,
                placa,
                'Normal' as situacao,
                'Suspenso' as situacao_anterior,
                origem_data_fim as origem
                1 as priority
            from {{ this }}
            where data_fim_suspensao is not null

            union all

        {% endif %}

        select
            data, id_veiculo, placa, situacao, situacao_anterior, origem, 0 as priority
        from licenciamento_situacao_dia
    ),
    datas_deduplicadas as (
        select *
        from datas_completas
        qualify
            row_number() over (partition by data, id_veiculo, placa order by priority)
            = 1
    ),
    inicio_filtrado as (
        select *
        from datas_deduplicadas
        qualify
            (
                lag(situacao) over (partition by id_veiculo, placa order by data)
                != situacao
                or lag(situacao) over (partition by id_veiculo, placa order by data)
                is null
            )
            or (
                situacao_anterior = 'Suspenso'
                and (
                    origem = 'stu'
                    and (
                        lead(origem) over (partition by id_veiculo, placa order by data)
                        = 'stu'
                        or (
                            lead(origem) over (
                                partition by id_veiculo, placa order by data
                            )
                            = 'lacre'
                            and lead(situacao) over (
                                partition by id_veiculo, placa order by data
                            )
                            = 'Suspenso'
                        )
                    )
                )
            )
    ),
    fim_filtrado as (
        select *
        from inicio_filtrado
        qualify
            (
                origem = 'stu'
                and (
                    lead(situacao) over (partition by id_veiculo, placa order by data)
                    != 'Suspenso'
                    or
                )
            )
    )
    data_inicio_fim as (
        select
            case when situacao = 'Suspenso' then data end as data_inicio_suspensao,
            case
                when situacao_anterior = 'Suspenso' then data
            end as data_fim_suspensao,
            id_veiculo,
            placa
        from licenciamento_situacao_dia
    ),
    novos_dados as (
        select
            data_inicio_suspensao,
            data_fim_suspensao,
            id_veiculo,
            placa,
            'staging' as fonte
        from data_inicio_fim
        where data_inicio_suspensao is not null
    )
    {% if is_incremental() %}
        ,
        dados_atuais as (select *, 'tratada' as fonte from {{ this }}),
        dados_completos as (
            select

            from dados_atuais a
            full outer join
                novos_dados n
                on a.placa = n.placa
                and a.id_veiculo = n.id_veiculo
                and a.data_inicio_suspensao
        )

    {% endif %}
