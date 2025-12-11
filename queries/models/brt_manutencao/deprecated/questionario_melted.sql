{{
    config(
        materialized="view",
    )
}}

select *
from
    (
        select
            ts as timestamp,
            estacao_brt as estacoes_brt,
            regexp_replace(split(pair, ':')[safe_offset(0)], r'^"|"$', '') id_problema,
            split(regexp_replace(split(pair, ':')[safe_offset(1)], r'^"|"$', ''), ' ')[
                safe_offset(0)
            ] seriedade
        from
            `rj-smtr.brt_manutencao.questionario` t,
            unnest(
                split(
                    regexp_replace(
                        regexp_replace(to_json_string(t), r'{|}', ''), r'","', '|'
                    ),
                    '|'
                )
            ) pair
    )
where
    not lower(id_problema) in (
        'ts',
        'estacao_brt',
        'email',
        'comentarios',
        'imagens',
        'numero',
        'nome_pesquisador',
        'imagens_comentarios'
    )
    and lower(id_problema) not like 'foto_%'
