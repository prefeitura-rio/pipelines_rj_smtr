{{
    config(
        materialized="table",
    )
}}

{% set max_pernas = 7 %}
{% set transacao = ref("transacao") %}

{% if execute and not flags.FULL_REFRESH %}

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
    matriz as (
        select *
        from {{ ref("matriz_integracao") }}
        where tipo_integracao = 'Transferência'
    ),
    transacao as (
        select
            *,
            if(
                cadastro_cliente = 'Não Cadastrado', hash_cartao, id_cliente
            ) as cliente_cartao
        from {{ transacao }}
        where
            tipo_transacao != "Gratuidade" and tipo_transacao_jae != 'Botoeira'
            {% if not flags.FULL_REFRESH %}
                {% if partitions | length > 0 %}
                    and data in ({{ partitions | join(", ") }})

                {% else %} and false
                {% endif %}
            {% endif %}
    ),
    transacao_mudanca_modo as (
        select
            *,
            case
                when
                    ifnull(
                        lag(modo) over (
                            partition by cliente_cartao order by datetime_transacao
                        ),
                        ''
                    )
                    != modo
                then 1
            end as separacao_bloco
        from transacao
    ),
    identificacao_bloco as (
        select
            *,
            sum(separacao_bloco) over (
                partition by cliente_cartao order by datetime_transacao
            ) as id_bloco
        from transacao_mudanca_modo
    ),
    transacao_modo_filtrado as (
        select * from identificacao_bloco where modo in ('VLT', 'BRT')
    ),
    lista_transferencia as (
        select
            *,
            array_agg(id_transacao) over (
                partition by cliente_cartao, id_bloco
                order by datetime_transacao
                rows between current row and {{ max_pernas - 1 }} following
            ) as proximas_transacoes
        from transacao_modo_filtrado
    ),
    transacao_unnest as (
        select lt.* except (proximas_transacoes), id_proxima_transacao
        from lista_transferencia lt, unnest(proximas_transacoes) as id_proxima_transacao
        where array_length(proximas_transacoes) > 1
    ),
    transacao_join as (
        select
            t1.id_transacao as id_transferencia,
            t1.cliente_cartao,
            t2.* except (cliente_cartao)
        from transacao_unnest t1
        join transacao t2 on t1.id_proxima_transacao = t2.id_transacao
    ),
    sequencia_transferencia as (
        select
            id_transferencia,
            row_number() over (
                partition by id_transferencia order by datetime_transacao
            ) as sequencia_transferencia,
            min(datetime_transacao) over (
                partition by id_transferencia
            ) as datetime_inicio_transferencia,
            lag(id_servico_jae) over (
                partition by id_transferencia order by datetime_transacao
            ) as id_servico_jae_origem,
            datetime_diff(
                datetime_transacao,
                min(datetime_transacao) over (partition by id_transferencia),
                minute
            ) as tempo_integracao,
            * except (id_transferencia)
        from transacao_join
    ),
    join_matriz as (
        select st.*, m.integracao
        from sequencia_transferencia st
        left join
            matriz m
            on st.sequencia_transferencia > 1
            and st.modo = m.modo_destino
            and (
                st.id_servico_jae = m.id_servico_jae_destino
                or m.id_servico_jae_destino is null
            )
            and (
                st.id_servico_jae_origem = m.id_servico_jae_origem
                or m.id_servico_jae_origem is null
            )
            and tempo_integracao <= tempo_integracao_minutos
            and st.id_servico_jae_origem != st.id_servico_jae
        qualify
            min(m.indicador_integracao) over (partition by st.id_transferencia) is true
    ),
    transacao_valida as (
        select *
        from join_matriz
        where integracao is not null or sequencia_transferencia = 1
        qualify
            sequencia_transferencia = row_number() over (
                partition by id_transferencia order by datetime_transacao
            )
    ),
    transferencia_sem_perna_unica as (
        select
            data,
            id_transferencia,
            cliente_cartao,
            datetime_inicio_transferencia,
            sequencia_transferencia,
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
            valor_transacao
        from transacao_valida
        qualify count(*) over (partition by id_transferencia) > 1
    )
select *
from transferencia_sem_perna_unica
