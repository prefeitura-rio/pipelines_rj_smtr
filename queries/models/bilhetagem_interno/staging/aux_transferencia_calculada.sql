{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set max_pernas = 6 %}

with
    transacao as (
        select
            *,
            if(
                cadastro_cliente = 'Não Cadastrado', hash_cartao, id_cliente
            ) as cliente_cartao
        from
            {# {{ ref("transacao") }} #}
            `rj-smtr.bilhetagem.transacao`
        where
            data between '2025-08-06' and '2025-08-08'
            and tipo_transacao != "Gratuidade"
            and tipo_transacao_jae != 'Botoeira'
    ),
    transacao_mudanca_modo as (
        select
            *,
            case
                when
                    ifnull(
                        lag(modo) over (
                            partition by id_cliente order by datetime_transacao
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
                partition by id_cliente order by datetime_transacao
            ) as id_bloco
        from transacao_mudanca_modo
    ),
    transacao_modo_filtrado as (
        select * from identificacao_bloco where modo in ('VLT', 'BRT')
    ),
    matriz as (
        select *
        from {{ ref("matriz_integracao") }}
        where tipo_integracao = 'Transferência'
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
        qualify
            min(m.indicador_integracao) over (partition by st.id_transferencia) is true
    ),
    transacao_valida as (
        select *
        from join_matriz
        qualify
            countif(integracao is null) over (partition by id_transferencia) = 1
            and count(*) over (partition by id_transferencia) > 1
    ),
    {% for i in range(max_pernas) %}
        {% if i == 0 %} {% set tabela_dados = "transacao_valida" %}
        {% else %}
            {% set last_i = i | int - 1 %} {% set tabela_dados = "remocao_" ~ last_i %}
        {% endif %}

        indicador_remover_{{ i }} as (
            select
                *,
                row_number() over (
                    partition by id_transacao order by datetime_inicio_transferencia
                )
                > 1 as indicador_remover
            from {{ tabela_dados }}
        ),
        max_indicador_remover_{{ i }} as (
            select
                id_transferencia,
                cliente_cartao,
                datetime_inicio_transferencia,
                max(indicador_remover) indicador_remover,
            from indicador_remover_{{ i }}
            group by 1, 2, 3
        ),
        validacao_{{ i }} as (
            select * except (indicador_remover),
            from indicador_remover_{{ i }}
            qualify
                lag(indicador_remover) over (
                    partition by cliente_cartao order by datetime_inicio_transferencia
                )
                is false
                and indicador_remover is true
        ),
        remocao_{{ i }} as (
            select i.*
            from {{ tabela_dados }} i
            left join validacao_{{ i }} v using (id_transferencia)
            where v.id_transferencia is null
        ),
    {% endfor %}
    final as (
        select
            data,
            id_transferencia,
            sequencia_transferencia,
            id_transacao,
            datetime_transacao,
            datetime_processamento,
            modo,
            id_consorcio,
            consorcio,
            id_servico_jae,
            servico_jae,
            sentido,
            descricao_servico_jae,
            tipo_transacao_jae,
            tipo_transacao,
            produto,
            produto_jae,
            id_cliente,
            hash_cartao,
            id_validador,
            valor_transacao
        from remocao_{{ max_pernas - 1 }}
        where date(datetime_inicio_transferencia) = '2025-08-07'

    )
select *
from final
