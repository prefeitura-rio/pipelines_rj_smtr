{{
    config(
        materialized="table",
    )
}}

with
    nome_celular_formatado as (
        select
            `rj-crm-registry.udf.FORMAT_NAME`(nome, true) as nome,
            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(
                concat(
                    ifnull(telefone.principal.ddi, "55"),
                    ifnull(telefone.principal.ddd, "21"),
                    telefone.principal.valor
                )
            ) as celular_disparo
        from {{ ref("aux_disparos_whatsapp_caso_1_2") }}
    )

select to_json_string(struct(celular_disparo, nome as NOME)) as vars
from nome_celular_formatado
{# left join
    `rj-crm-registry.crm_whatsapp.telefone_sem_whatsapp` blacklist
    on blacklist.telefone = nome_celular_formatado.celular_formatado #}
{# left join
    `rj-crm-registry.disparos_staging.disparos_efetuados` disparos
    on disparos.to = base.celular_formatado
    and disparos.id_hsm = {str(jae_hsm_id)} #}
where
    celular_disparo is not null
    -- and (remover telefones que estejam na blacklist)
    -- and (remover telefones que já tenham recebido o disparo)

