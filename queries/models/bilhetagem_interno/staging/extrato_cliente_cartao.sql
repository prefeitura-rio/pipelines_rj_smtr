with
    extrato_cliente as (
        select
            l.id_conta,
            l.cd_cliente as id_cliente,
            j.nome as nome_cliente,
            j.documento as nome_documento,
            l.id_lancamento,
            l.dt_lancamento as data_lancamento,
            l.vl_lancamento as valor_lancamento,
            l.ds_tipo_movimento as tipo_movimento,
            l.timestamp_captura as datetime_captura,
            case
                when regexp_contains(id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
                then split(id_conta, ".")[offset(3)]
            end as nr_logico_midia_tratado,
            timestamp_captura
        from {{ ref("staging_lancamento") }}
        -- from `rj-smtr-dev.adriano__bilhetagem_interno_staging.lancamento` l
        left join
            `rj-smtr.cadastro_interno.cliente_jae` j on j.id_cliente = l.cd_cliente
        where
            (
                regexp_contains(id_conta, r'^2\.2\.1\.[A-Za-z0-9]+\.(1|2|6)$')
                or regexp_contains(id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
            )
            and data = '2025-09-05'
        qualify
            row_number() over (
                partition by id_lancamento, id_conta order by timestamp_captura desc
            )
            = 1
    )
select *
from extrato_cliente
