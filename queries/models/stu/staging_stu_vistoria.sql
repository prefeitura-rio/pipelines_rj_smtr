{{ config(alias="vistoria") }}

select
    id_vistoria,
    anoexe as ano_exercicio,
    safe_cast(json_value(content, '$.tptran1') as float64) as id_tipo_transporte,
    safe_cast(json_value(content, '$.tptran2') as float64) as id_tipo_transporte_2,
    safe_cast(json_value(content, '$.guia') as string) as guia,
    safe_cast(json_value(content, '$.selo') as float64) as selo,
    safe_cast(json_value(content, '$.anoselo') as float64) as ano_selo,
    safe_cast(json_value(content, '$.darm') as float64) as darm,
    safe_cast(json_value(content, '$.anodarm') as float64) as ano_darm,
    safe_cast(json_value(content, '$.placa') as string) as placa,
    safe_cast(json_value(content, '$.ordem') as string) as ordem,
    safe_cast(json_value(content, '$.cod_fiscal') as string) as cod_fiscal,
    safe_cast(json_value(content, '$.tptran') as int64) as tptran,
    safe_cast(json_value(content, '$.tpperm') as int64) as tpperm,
    safe_cast(json_value(content, '$.termo') as int64) as termo,
    safe_cast(json_value(content, '$.ratr') as float64) as ratr,
    safe_cast(json_value(content, '$.visobrig') as int64) as vistoria_obrigatoria,
    safe_cast(json_value(content, '$.num_vistoria') as int64) as numero_vistoria,
    safe_cast(json_value(content, '$.dt_vistoria') as datetime) as data_vistoria,
    safe_cast(json_value(content, '$.matr_usuario') as string) as matricula_usuario,
    safe_cast(json_value(content, '$.data_usuario') as datetime) as data_usuario,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(json_value(content, '$.codrec') as int64) as codrec,
    safe_cast(json_value(content, '$.Emissao') as string) as emissao,
    safe_cast(
        json_value(content, '$._datetime_execucao_flow') as datetime
    ) as datetime_execucao_flow,
    timestamp_captura
from {{ source("source_stu", "vistoria") }}
