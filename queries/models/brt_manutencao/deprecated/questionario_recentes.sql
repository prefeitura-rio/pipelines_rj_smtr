{{
    config(
        materialized="view",
    )
}}

select agg.table.*
from
    (
        select
            nome_estacao,
            id_problema,
            array_agg(struct(table) order by dt desc)[safe_offset(0)] agg
        from {{ ref("questionario_melted_completa") }} table
        group by nome_estacao, id_problema
    )
