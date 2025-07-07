select data, servico, tipo, status
from {{ ref("aux_servico_dia_atipico") }}
where incorporado_datalake_house is not true
