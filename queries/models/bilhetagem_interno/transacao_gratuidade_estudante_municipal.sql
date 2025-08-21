{{
    config(
        materialized="view",
    )
}}


select
    * except (
        documento_operadora,
        id_cliente,
        hash_cliente,
        documento_cliente,
        hash_cartao,
        saldo_cartao,
        subtipo_usuario_protegido
    )
from {{ ref("transacao") }}
where
    tipo_transacao_jae in ('Gratuidade', 'Integração gratuidade')
    and subtipo_usuario = 'Estudante Municipal'
