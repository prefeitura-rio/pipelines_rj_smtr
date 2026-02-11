
{{
    config(
        materialized="view",
    )
}}

with dados_consolidados as (
  select
    data,
    id_veiculo,
    ano_fabricacao,
    carroceria,
    indicador_ar_condicionado,
    nome_chassi,
    permissao,
    placa,
    tipo_veiculo,
    substr(id_veiculo, 2, 3) as cod_veiculo
  from `rj-smtr.veiculo.licenciamento` -- tabela antiga
  where 
    permissao in ("22.100002-3", "22.100003-2", "22.100001-4", "22.100004-1")
    and data <= "2025-03-31"
    and lower(tipo_veiculo) not like "%rod%"
  
  union all

  select
    data,
    id_veiculo,
    ano_fabricacao,
    carroceria,
    indicador_ar_condicionado,
    nome_chassi,
    permissao,
    placa,
    tipo_veiculo,
    substr(id_veiculo, 2, 3) as cod_veiculo
  from `rj-smtr.cadastro.veiculo_licenciamento_dia` -- tab depois de 01/04/25
  where 
    permissao in ("22.100002-3", "22.100003-2", "22.100001-4", "22.100004-1")
    and data >= "2025-04-01"
    and lower(tipo_veiculo) not like "%rod%"
  qualify row_number() over (
    partition by data, id_veiculo
    order by data_processamento desc
  ) = 1
)

select
  data,
  id_veiculo,
  ano_fabricacao,
  carroceria,
  indicador_ar_condicionado,
  nome_chassi,
  permissao,
  placa,
  tipo_veiculo,
  cod_veiculo,
  case
    when cod_veiculo in ('100', '101', '102', '103', '104') then 'Paranapuan'
    when cod_veiculo in ('105', '106', '107', '108', '109') then 'Penha Rio'
    when cod_veiculo in ('115', '116', '117', '118', '119') then 'Vila Real'
    when cod_veiculo in ('120', '121', '122', '123', '124') then 'Recreio'
    when cod_veiculo in ('125', '126', '127', '128', '129') then 'Normandy'
    when cod_veiculo in ('130', '131', '132', '133', '135', '136', '137', '138', '139') then 'Barra'
    when cod_veiculo in ('170', '171', '172', '173', '174', '175', '176', '177', '178', '179') then 'Palmares'
    when cod_veiculo in ('180', '181', '182', '183', '184', '185', '186', '187', '188', '189') then 'Mendanha'
    when cod_veiculo in ('255', '256', '257', '258', '259') then 'Matias'
    when cod_veiculo in ('270', '271', '272', '273', '274') then 'Caprichosa'
    when cod_veiculo in ('275', '276', '277', '278', '279') then 'Vila Isabel'
    when cod_veiculo in ('285', '286', '287', '288', '289') then 'Ideal'
    when cod_veiculo in ('290', '291', '292', '293', '294', '295', '296', '297', '298', '299') then 'Braso Lisboa'
    when cod_veiculo in ('300', '301', '302', '303', '304', '305', '306', '307', '308', '309') then 'Futuro'
    when cod_veiculo in ('310', '311', '312', '313', '314', '315', '316', '317', '318', '319') then 'VG'
    when cod_veiculo in ('325', '326', '327', '328', '329') then 'Pavunense'
    when cod_veiculo in ('330', '331', '332', '333', '334', '335', '336', '337', '338', '339') then 'Sancetur'
    when cod_veiculo in ('410', '411', '412', '413', '414', '415', '416', '417', '418', '419') then 'Real'
    when cod_veiculo in ('445', '446', '447', '448', '449') then 'Três Amigos'
    when cod_veiculo in ('470', '471', '472', '473', '474', '475', '476', '477', '478', '479') then 'Redentor'
    when cod_veiculo in ('480', '481', '482', '483', '484', '485', '486', '487', '488', '489') then 'Alpha'
    when cod_veiculo in ('500', '501', '502', '503', '504', '505', '506', '507', '508', '509') then 'Tijuca'
    when cod_veiculo in ('515', '516', '517', '518', '519') then 'Novacap'
    when cod_veiculo in ('535', '536', '537', '538', '539') then 'Campo Grande'
    when cod_veiculo in ('580', '581', '582', '583', '584', '585', '586', '587', '588', '589') then 'Lourdes'
    when cod_veiculo in ('630', '631', '632', '633', '634', '635', '636', '637', '638', '639') then 'GIRE'
    when cod_veiculo in ('710', '711', '712', '713', '714') then 'Verdun'
    when cod_veiculo in ('715', '716', '717', '718', '719') then 'Graças'
    when cod_veiculo in ('720', '721', '722', '723', '724', '725', '726', '727', '728', '729') then 'Transurb'
    when cod_veiculo in ('860', '861', '862', '863', '864', '865', '866', '867', '868', '869') then 'Jabour'
    when cod_veiculo in ('870', '871', '872', '873', '874', '875', '876', '877', '878', '879') then 'Pégaso'
    else 'Desconhecida'
  end as operadora
from dados_consolidados