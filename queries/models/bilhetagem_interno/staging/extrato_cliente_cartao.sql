with
    extrato_cliente as (
        select
            array_to_string(
                array_concat(
                    [
                        split(id_conta, ".")[offset(0)],
                        split(id_conta, ".")[offset(1)],
                        split(id_conta, ".")[offset(2)]
                    ],
                    [split(id_conta, ".")[offset(4)]]
                ),
                "."
            ) as id_conta,
            cd_cliente as id_cliente,
            id_lancamento,
            data,
            vl_lancamento as valor_lancamento,
            split(id_conta, ".")[offset(3)] as nr_logico_midia_tratado,
            timestamp_captura
        from `rj-smtr-dev.adriano__bilhetagem_interno_staging.lancamento`
        where
            regexp_contains(id_conta, r'^2\.2\.1\.[A-Za-z0-9]+\.(1|2|6)$')
            or regexp_contains(id_conta, r'^2\.2\.3\.[A-Za-z0-9]+\.1$')
            and data = '2025-09-05'
    ),
    extrato_cliente_deduplicada as (
        select * except (rn)
        from
            (
                select
                    * except (timestamp_captura),
                    row_number() over (
                        partition by id_lancamento, id_conta
                        order by timestamp_captura desc
                    ) as rn
                from extrato_cliente
            -- {% if is_incremental() -%} where {{ incremental_filter }} {%- endif %}
            )
        where rn = 1
    )
select *
from extrato_cliente_deduplicada
