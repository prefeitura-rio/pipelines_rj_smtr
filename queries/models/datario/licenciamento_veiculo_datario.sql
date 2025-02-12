{{ config(alias="licenciamento_veiculo") }}
select *
{# from {{ ref("licenciamento") }} #}
from `rj-smtr.veiculo.licenciamento`
