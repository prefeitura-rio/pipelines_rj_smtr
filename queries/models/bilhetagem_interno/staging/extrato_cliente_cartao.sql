with
    extrato_cliente as (
        select
            l.id_conta,
            l.cd_cliente as id_cliente,
            j.nome as nome_cliente,
            j.documento as nr_documento,
            j.tipo_documento,
            l.id_lancamento,
            l.dt_lancamento as data_lancamento,
            l.vl_lancamento as valor_lancamento,
            l.ds_tipo_movimento as tipo_movimento,
            l.timestamp_captura as datetime_captura,
            case
                when regexp_contains(l.id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
                then split(l.id_conta, ".")[offset(3)]
            end as nr_logico_midia
        from {{ ref("staging_lancamento") }} as l
        -- from `rj-smtr-dev.adriano__bilhetagem_interno_staging.lancamento` 
        left join
            `rj-smtr.cadastro_interno.cliente_jae` j on j.id_cliente = l.cd_cliente
        where
            (
                regexp_contains(l.id_conta, r'^2\.2\.1\.[A-Za-z0-9]+\.(1|2|6)$')
                or regexp_contains(l.id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
            )
        qualify
            row_number() over (
                partition by l.id_lancamento, l.id_conta
                order by l.timestamp_captura desc
            )
            = 1
    )
select *
from extrato_cliente
