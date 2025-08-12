{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set max_pernas = 3 %}
{% set transacao = ref("transacao") %}
{% set transferencia = ref("aux_transferencia_calculada") %}

{% if execute and is_incremental() %}

    {% set partitions_query %}
        with particoes as (
            select
                parse_date("%Y%m%d", partition_id) as particao
                {# concat("'", parse_date("%Y%m%d", partition_id), "'") as particao #}
            from
                `{{ transacao.database }}.{{ transacao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ transacao.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and (datetime("{{var('date_range_end')}}"))
            ),
        particoes_adjacentes as (
            select date_add(particao, interval 1 day) as particao
            from particoes

            union distinct

            select date_sub(particao, interval 1 day) as particao
            from particoes
        )
        select concat("'", particao, "'") as particao from particoes

        union distinct

        select concat("'", particao, "'") as particao from particoes_adjacentes

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

with
    transacao as (
        select
            *,
            if
            (
                cadastro_cliente = 'Não Cadastrado', hash_cartao, id_cliente
            ) as cliente_cartao
        from {{ ref("transacao") }}
        where
            tipo_transacao != "Gratuidade" and tipo_transacao_jae != 'Botoeira'
            {% if is_incremental() %}
                {% if partitions | length > 0 %}
                    and data in ({{ partitions | join(", ") }})
                {% else %} and false
                {% endif %}
            {% endif %}
    ),
    matriz as (
        select *
        from {{ ref("matriz_integracao") }}
        where tipo_integracao = 'Integração'
    ),
    transferencia as (
        select *
        from {{ transferencia }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    transacao_transferencia as (
        select * from transferencia where sequencia_transferencia > 1
    ),
    transacao_sem_transferencia as (
        select t.*
        from transacao t
        left join transacao_transferencia tt using (id_transacao)
        where tt.id_transacao is null
    ),
    lista_integracao as (
        select
            *,
            array_agg(id_transacao) over (
                partition by cliente_cartao
                order by datetime_transacao
                rows between current row and {{ max_pernas - 1 }} following
            ) as proximas_transacoes
        from transacao_sem_transferencia
    ),
    transacao_unnest as (
        select li.* except (proximas_transacoes), id_proxima_transacao
        from lista_integracao li, unnest(proximas_transacoes) as id_proxima_transacao
        where array_length(proximas_transacoes) > 1
    ),
    transacao_join as (
        select
            t1.id_transacao as id_integracao,
            t1.cliente_cartao,
            t2.* except (cliente_cartao),
            case
                when t2.modo = 'Van'
                then t2.consorcio
                when
                    t2.modo = 'Ônibus'
                    and not (
                        length(ifnull(regexp_extract(t2.servico_jae, r"[0-9]+"), ""))
                        = 4
                        and ifnull(regexp_extract(t2.servico_jae, r"[0-9]+"), "")
                        like "2%"
                    )
                then 'SPPO'
                else t2.modo
            end as modo_join
        from transacao_unnest t1
        join transacao t2 on t1.id_proxima_transacao = t2.id_transacao
    ),
    sequencia_integracao as (
        select
            id_integracao,
            row_number() over (
                partition by id_integracao order by datetime_transacao
            ) as sequencia_integracao,
            min(datetime_transacao) over (
                partition by id_integracao
            ) as datetime_inicio_integracao,
            string_agg(modo_join, '-') over (
                partition by id_integracao
                order by datetime_transacao
                rows between unbounded preceding and 1 preceding
            ) as integracao_origem,
            lag(id_servico_jae) over (
                partition by id_integracao order by datetime_transacao
            ) as id_servico_jae_origem,
            datetime_diff(
                datetime_transacao,
                min(datetime_transacao) over (partition by id_integracao),
                minute
            ) as tempo_integracao,
            * except (id_integracao)
        from transacao_join
    ),
    join_matriz as (
        select si.*, m.integracao
        from sequencia_integracao si
        left join
            matriz m
            on coalesce(m.modo_origem, m.integracao_origem) = si.integracao_origem
            and si.modo_join = m.modo_destino
            and (
                si.id_servico_jae = m.id_servico_jae_destino
                or m.id_servico_jae_destino is null
            )
            and (
                si.id_servico_jae_origem = m.id_servico_jae_origem
                or m.id_servico_jae_origem is null
            )
            and tempo_integracao <= tempo_integracao_minutos
        where m.integracao is not null or si.sequencia_integracao = 1
        qualify
            min(m.indicador_integracao) over (partition by si.id_integracao) is true
            and concat(id_servico_jae, sentido) not in unnest(
                array_agg(concat(id_servico_jae, sentido)) over (
                    partition by id_integracao
                    rows between unbounded preceding and 1 preceding
                )
            )
    ),
    integracao_sem_perna_unica as (
        select * from join_matriz qualify count(*) over (partition by id_integracao) > 1
    ),
    integracao_completa as (
        select
            data,
            id_integracao,
            datetime_inicio_integracao,
            sequencia_integracao,
            {# if(
                sequencia_integracao = 1, 'Primeira perna', tipo_integracao
            ) as tipo_integracao, #}
            id_transacao,
            datetime_transacao,
            datetime_processamento,
            modo,
            id_consorcio,
            consorcio,
            id_servico_jae,
            servico_jae,
            descricao_servico_jae,
            sentido,
            id_veiculo,
            id_validador,
            id_cliente,
            hash_cartao,
            cadastro_cliente,
            produto,
            produto_jae,
            tipo_transacao_jae,
            tipo_transacao,
            tipo_usuario,
            meio_pagamento,
            meio_pagamento_jae,
            valor_transacao,
            cliente_cartao
        from integracao_sem_perna_unica

        {% if is_incremental() and partitions | length > 0 %}
            union all

            select *
            from {{ this }}
            where
                data in ({{ partitions | join(", ") }})
                and id_integracao
                not in (select id_integracao from integracao_sem_perna_unica)
        {% endif %}
    ),
    {% for i in range(max_pernas + 1) %}
        {% if i == 0 %} {% set tabela_dados = "integracao_completa" %}
        {% else %}
            {% set last_i = i | int - 1 %} {% set tabela_dados = "remocao_" ~ last_i %}
        {% endif %}

        indicador_remover_{{ i }} as (
            select
                *,
                row_number() over (
                    partition by id_transacao order by datetime_inicio_integracao
                )
                > 1 as indicador_remover
            from {{ tabela_dados }}
        ),
        max_indicador_remover_{{ i }} as (
            select
                * except (indicador_remover),
                max(indicador_remover) over (
                    partition by id_integracao
                ) as indicador_remover,
            from indicador_remover_{{ i }}
        ),
        validacao_{{ i }} as (
            select * except (indicador_remover),
            from max_indicador_remover_{{ i }}
            qualify
                lag(indicador_remover) over (
                    partition by cliente_cartao order by datetime_inicio_integracao
                )
                is false
                and indicador_remover is true
        ),
        remocao_{{ i }} as (
            select i.*
            from {{ tabela_dados }} i
            left join validacao_{{ i }} v using (id_integracao)
            where v.id_integracao is null
        ),
    {% endfor %}
    final as (select * from remocao_{{ max_pernas }})
select *
from final
