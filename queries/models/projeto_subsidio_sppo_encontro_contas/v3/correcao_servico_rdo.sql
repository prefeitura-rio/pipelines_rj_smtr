select data, servico, servico_corrigido, tipo
from {{ ref("staging_encontro_contas_correcao_servico_rdo") }}
qualify
    row_number() over (partition by data, servico, tipo order by data_resposta desc) = 1
