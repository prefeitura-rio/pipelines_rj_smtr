select
    extract(year from data) as ano,
    extract(month from data) as mes,
    sum(
        qtd_buc_1_perna
        + qtd_buc_2_perna_integracao
        + qtd_buc_supervia_1_perna
        + qtd_buc_supervia_2_perna_integracao
    ) / sum(
        qtd_grt_idoso
        + qtd_grt_especial
        + qtd_grt_estud_federal
        + qtd_grt_estud_estadual
        + qtd_grt_estud_municipal
        + qtd_grt_rodoviario
        + qtd_grt_passe_livre_universitario
        + qtd_buc_1_perna
        + qtd_buc_2_perna_integracao
        + qtd_buc_supervia_1_perna
        + qtd_buc_supervia_2_perna_integracao
        + qtd_cartoes_perna_unica_e_demais
        + qtd_pagamentos_especie
    ) as percentual_de_integracoes

from {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }}
group by 1, 2
order by 1, 2
