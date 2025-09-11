{{
    config(
        materialized="view",
    )
}}


select
    t.data,
    t.hora,
    t.datetime_transacao,
    t.id_transacao,
    t.modo,
    t.tipo_documento_cliente,
    t.documento_cliente,
    c.nome as nome_cliente,
    t.tipo_transacao,
    t.tipo_transacao_jae,
    t.tipo_usuario,
    t.subtipo_usuario
from {{ ref("transacao") }} t
join {{ ref("cliente_jae") }} c using (id_cliente)
join {{ ref("estudante") }} e using (id_cliente)
where
    t.tipo_transacao_jae in ('Gratuidade', 'Integração gratuidade')
    and t.tipo_usuario = "Estudante"
    and t.subtipo_usuario = 'Ensino Básico Municipal'
