select
    * except (
        indicador_atipico,
        indicador_ausencia_receita_tarifaria,
        indicador_data_excecao,
        datas_servico,
        datas_servico_pod_maior_80,
        datas_servico_pod_menor_80
    )
from {{ ref("aux_balanco_servico_dia") }}
where
    not (
        indicador_atipico
        or indicador_ausencia_receita_tarifaria
        or indicador_data_excecao
    )
