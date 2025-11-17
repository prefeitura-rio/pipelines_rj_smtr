{{
    config(
        materialized="view",
    )
}}

with
    todos_problemas_estacoes as (
        select
            t1.codigo_estacao as id_estacao,
            t1.nome_estacao,
            t1.ativa as ativa_estacao,
            t1.corredor as corredor_estacao,
            t2.id_problema,
            t2.nome_problema,
            t2.categoria as categoria_problema,
            t2.id_responsavel
        from `rj-smtr.brt_manutencao.estacoes` t1
        join `rj-smtr.brt_manutencao.problemas` t2 on true
        where t1.ativa = '1'
    )
select
    date(timestamp) dt,
    floor(timestamp_diff(current_timestamp(), timestamp, day)) idade_resposta_dia,
    floor(
        timestamp_diff(current_timestamp(), timestamp, day) / 7
    ) idade_resposta_semana,
    coalesce(t3.estacoes_brt, t4.nome_estacao) nome_estacao,
    ativa_estacao,
    corredor_estacao,
    categoria_problema,
    nome_problema,
    coalesce(t3.id_problema, t4.id_problema) id_problema,
    id_responsavel,
    coalesce(split(t3.seriedade, ' ')[safe_offset(0)], 'Sem Avaliação') seriedade
from {{ ref("questionario_melted") }} t3
full join
    todos_problemas_estacoes t4
    on t3.estacoes_brt = t4.nome_estacao
    and t3.id_problema = t4.id_problema
