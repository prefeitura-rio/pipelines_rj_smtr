-- depends_on: {{ ref('view_passageiro_tile_hora') }}
with
    servicos as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_servico_jae order by data_inicio_vigencia
                    ) as rn
                from {{ ref("servicos") }}
            )
        where rn = 1
    )
select
    p.data,
    p.hora,
    p.modo,
    p.consorcio,
    p.id_servico_jae,
    s.servico,
    s.descricao_servico,
    concat(s.servico, ' - ', s.descricao_servico) as nome_completo_servico,
    s.latitude as latitude_servico,
    s.longitude as longitude_servico,
    p.sentido,
    p.cadastro_cliente,
    p.produto,
    p.tipo_transacao,
    p.tipo_usuario,
    p.meio_pagamento,
    p.quantidade_passageiros
from {{ ref("passageiro_hora") }} p
left join servicos s using (id_servico_jae)
