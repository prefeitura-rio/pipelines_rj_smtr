{{
    config(
        materialized="view",
    )
}}
with
    satisfacao_problema as (
        select
            t3.nome_problema,
            t3.id_responsavel,
            t3.categoria_problema,
            sum(case when t4.id_seriedade = 'satisfatorio' then 1 else 0 end)
            / count(*) satisfacao
        from {{ ref("questionario_recentes") }} t3
        join `rj-smtr.brt_manutencao.seriedade` t4 on t3.seriedade = t4.nome_seriedade
        group by t3.nome_problema, t3.id_responsavel, categoria_problema
        order by satisfacao desc
    )
select
    nome_problema,
    categoria_problema,
    t1.id_responsavel,
    t1.nome_exibicao_responsavel,
    satisfacao * 100 satisfacao
from satisfacao_problema t0
join `rj-smtr.brt_manutencao.responsaveis` t1 on t0.id_responsavel = t1.id_responsavel
