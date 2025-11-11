{{
    config(
        materialized="view",
    )
}}
with
    satisfacao_estacao as (
        select
            t3.id_responsavel,
            t3.nome_estacao,
            array_agg(nome_seriedade order by ordem_seriedade desc limit 1)[
                offset(0)
            ] seriedade,
            max(ordem_seriedade) ordem_seriedade
        from {{ ref("questionario_recentes") }} t3
        join `rj-smtr.brt_manutencao.seriedade` t4 on t3.seriedade = t4.nome_seriedade
        group by t3.id_responsavel, t3.nome_estacao
        order by t3.id_responsavel, nome_estacao
    )
select
    t1.id_responsavel,
    t2.nome_exibicao_responsavel,
    t2.email_responsavel,
    t2.telefone_responsavel,
    nome_estacao,
    seriedade,
    split(seriedade, '(')[safe_offset(0)] seriedade_simples,
    ordem_seriedade
from satisfacao_estacao t1
join `rj-smtr.brt_manutencao.responsaveis` t2 on t1.id_responsavel = t2.id_responsavel
