-- depends_on: {{ ref('matriz_integracao') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key="id_transacao",
    )
}}

with
    integracao_transacao_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id order by timestamp_captura desc
                    ) as rn
                from
                    {{ ref("staging_integracao_transacao") }}
                    {# `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.integracao_transacao` #}
                {% if is_incremental() -%}
                    where
                        date(data) between date("{{var('date_range_start')}}") and date(
                            "{{var('date_range_end')}}"
                        )
                        and timestamp_captura
                        between datetime("{{var('date_range_start')}}") and datetime(
                            "{{var('date_range_end')}}"
                        )
                {%- endif %}
            )
        where rn = 1
    ),
    integracao_melt as (
        select
            extract(date from im.data_transacao) as data,
            extract(hour from im.data_transacao) as hora,
            i.data_inclusao as datetime_inclusao,
            i.data_processamento as datetime_processamento_integracao,
            i.timestamp_captura as datetime_captura,
            i.id as id_integracao,
            im.sequencia_integracao,
            im.data_transacao as datetime_transacao,
            im.id_tipo_modal,
            im.id_consorcio,
            im.id_operadora,
            im.id_linha,
            im.id_transacao,
            im.sentido,
            im.perc_rateio,
            im.valor_rateio_compensacao,
            im.valor_rateio,
            im.valor_transacao,
            i.valor_transacao_total,
            i.tx_adicional as texto_adicional
        from
            integracao_transacao_deduplicada i,
            -- Transforma colunas com os dados de cada transação da integração em
            -- linhas diferentes
            unnest(
                [
                    {% for n in range(var("quantidade_integracoes_max")) %}
                        struct(
                            {% for column, column_config in var(
                                "colunas_integracao"
                            ).items() %}
                                {% if column_config.select %}
                                    {{ column }}_t{{ n }} as {{ column }},
                                {% endif %}
                            {% endfor %}
                            {{ n + 1 }} as sequencia_integracao
                        )
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                ]
            ) as im
    ),
    integracao_rn as (
        select
            i.data,
            i.hora,
            i.datetime_processamento_integracao,
            i.datetime_captura,
            i.datetime_transacao,
            timestamp_diff(
                i.datetime_transacao,
                lag(i.datetime_transacao) over (
                    partition by i.id_integracao order by sequencia_integracao
                ),
                minute
            ) as intervalo_integracao,
            i.id_integracao,
            i.sequencia_integracao,
            m.modo,
            dc.id_consorcio,
            dc.consorcio,
            do.id_operadora,
            do.operadora,
            i.id_linha as id_servico_jae,
            l.nr_linha as servico_jae,
            l.nm_linha as descricao_servico_jae,
            i.id_transacao,
            i.sentido,
            i.perc_rateio as percentual_rateio,
            i.valor_rateio_compensacao,
            i.valor_rateio,
            i.valor_transacao,
            i.valor_transacao_total,
            i.texto_adicional,
            '{{ var("version") }}' as versao,
            row_number() over (
                partition by id_integracao, id_transacao
                order by datetime_processamento_integracao desc
            ) as rn
        from integracao_melt i
        left join
            {{ source("cadastro", "modos") }} m
            on i.id_tipo_modal = m.id_modo
            and m.fonte = "jae"
        left join
            {{ ref("operadoras") }} do on i.id_operadora = do.id_operadora_jae
            {# `rj-smtr.cadastro.operadoras` do on i.id_operadora = do.id_operadora_jae #}
        left join
            {{ ref("consorcios") }} dc on i.id_consorcio = dc.id_consorcio_jae
            {# `rj-smtr.cadastro.consorcios` dc on i.id_consorcio = dc.id_consorcio_jae #}
        left join
            {{ ref("staging_linha") }} l
            {# `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha` l #}
            on i.id_linha = l.cd_linha
        where i.id_transacao is not null
    ),
    integracoes_teste_invalidas as (
        select distinct i.id_integracao
        from integracao_rn i
        left join
            {{ ref("staging_linha_sem_ressarcimento") }} l
            {# `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.linha_sem_ressarcimento` l #}
            on i.id_servico_jae = l.id_linha
        where l.id_linha is not null or i.data < "2023-07-17"
    )
select * except (rn)
from integracao_rn
where
    rn = 1
    and id_integracao not in (select id_integracao from integracoes_teste_invalidas)
