{{
    config(
        materialized="view",
    )
}}

{% set aux_particao_calculo_integracao = ref("aux_particao_calculo_integracao") %}

{% if execute and not flags.FULL_REFRESH %}

    {% set partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }}

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

select
    data,
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
    if
    (cadastro_cliente = 'Não Cadastrado', hash_cartao, id_cliente) as cliente_cartao,
    case
        when modo = 'Van'
        then consorcio
        when
            modo = 'Ônibus'
            and not (
                length(ifnull(regexp_extract(servico_jae, r"[0-9]+"), "")) = 4
                and ifnull(regexp_extract(servico_jae, r"[0-9]+"), "") like "2%"
            )
        then 'SPPO'
        when modo = 'BRT' and ifnull(tarifa_ida, tarifa_volta) > 4.7
        then 'BRT ESP'
        else modo
    end as modo_join
from {{ ref("transacao") }} t
left join
    {{ ref("aux_linha_tarifa") }} l
    on t.id_servico_jae = l.cd_linha
    and t.datetime_transacao >= l.dt_inicio_validade
    and (l.data_fim_validade is null or t.datetime_transacao < lt.data_fim_validade)
where
    tipo_transacao != "Gratuidade" and tipo_transacao_jae != 'Botoeira'
    {% if not flags.FULL_REFRESH %}
        {% if partitions | length > 0 %} and data in ({{ partitions | join(", ") }})
        {% else %} and false
        {% endif %}
    {% endif %}
group by all
