{{ config(alias="licenciamento_veiculo") }}

select *
from {{ ref("licenciamento") }} full outer
union all by name
select *
from {{ ref("veiculo_licenciamento_dia") }}
