{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_inicio_lacre",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    date(data) between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% set staging_veiculo_fiscalizacao_lacre = ref("staging_veiculo_fiscalizacao_lacre") %}

{% if execute and is_incremental() %}
    {% set partitions_query %}

        SELECT DISTINCT
            CONCAT("'", data_do_lacre, "'") AS data_do_lacre
        FROM
            {{ staging_veiculo_fiscalizacao_lacre }}
        WHERE
            {{ incremental_filter }}
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

with
    staging as (
        select *
        from {{ staging_veiculo_fiscalizacao_lacre }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by n_o_de_ordem, placa, data_do_lacre
                order by timestamp_captura desc
            )
            = 1
    ),
    particoes_completas as (
        select
            n_o_de_ordem as id_veiculo,
            placa,
            data_do_lacre as data_inicio_lacre,
            data_do_deslacre as data_fim_lacre,
            permissao as id_consorcio,
            consorcio,
            concat(
                rpad(regexp_replace(substring(no_do_auto, 1, 2), r'\W', ''), 2),
                '-',
                lpad(regexp_replace(substring(no_do_auto, 3), r'\W', ''), 8, '0')
            ) as id_auto_infracao,
            ultima_atualizacao as datetime_ultima_atualizacao_fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from staging

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select * except (versao)
            from {{ this }}
            where data_inicio_lacre in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    aux_datetime_ultima_atualizacao as (
        select
            id_veiculo,
            placa,
            data_inicio_lacre,
            case
                when
                    array_length(atualizacoes) = 1
                    or atualizacoes[0].datetime_ultima_atualizacao_fonte
                    = atualizacoes[1].datetime_ultima_atualizacao_fonte
                then atualizacoes[0].datetime_ultima_atualizacao
                else atualizacoes[1].datetime_ultima_atualizacao
            end as datetime_ultima_atualizacao
        from
            (
                select
                    id_veiculo,
                    placa,
                    data_inicio_lacre,
                    array_agg(
                        struct(
                            datetime_ultima_atualizacao_fonte
                            as datetime_ultima_atualizacao_fonte,
                            datetime_ultima_atualizacao as datetime_ultima_atualizacao
                        )
                        order by datetime_ultima_atualizacao
                    ) as atualizacoes
                from particoes_completas
                group by all
            )
    )
select
    p.* except (datetime_ultima_atualizacao),
    a.datetime_ultima_atualizacao,
    '{{ var("version") }}' as versao
from particoes_completas p
join aux_datetime_ultima_atualizacao a using (id_veiculo, placa, data_inicio_lacre)
where
    data_fim_lacre > '{{ var("data_inicial_veiculo_fiscalizacao_lacre") }}'
    or data_fim_lacre is null
qualify
    row_number() over (
        partition by data_inicio_lacre, id_veiculo, placa
        order by datetime_ultima_atualizacao_fonte desc
    )
    = 1
