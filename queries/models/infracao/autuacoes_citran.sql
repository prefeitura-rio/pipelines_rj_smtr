
{{ config(
        materialized='view'
  )
}}


SELECT
  Cod__Detran,
  data,
  SAFE_CAST(JSON_VALUE(content,'$.Hora') AS STRING) hora,
  IF(JSON_VALUE(content, '$.DtLimDP') != '', SAFE_CAST(PARSE_DATE('%d/%m/%Y', JSON_VALUE(content,'$.DtLimDP')) AS STRING), NULL) data_limite_defesa_previa, -- data
  IF(JSON_VALUE(content, '$.DtLimR') != '', SAFE_CAST(PARSE_DATE('%d/%m/%Y', JSON_VALUE(content,'$.DtLimR')) AS STRING), NULL) data_limite_recurso, -- data
  SAFE_CAST(JSON_VALUE(content,'$.Situacao Atual') AS STRING) situacao_atual, 
  SAFE_CAST(JSON_VALUE(content,'$.St. Infracao') AS STRING) status_infracao, 
  SAFE_CAST(JSON_VALUE(content,'$.Multa') AS STRING) codigo_enquadramento, 
  SAFE_CAST(JSON_VALUE(content,'$.DsInf') AS STRING) tipificacao_resumida, 
  SAFE_CAST(JSON_VALUE(content,'$.Po') AS STRING) pontuacao, 
  SAFE_CAST(JSON_VALUE(content,'$.Tipo') AS STRING) tipo_veiculo, 
  SAFE_CAST(JSON_VALUE(content,'$.Marca') AS STRING) descricao_veiculo, 
  SAFE_CAST(JSON_VALUE(content,'$.Esp') AS STRING)especie_veiculo, 
  SAFE_CAST(JSON_VALUE(content,'$.CDUF') AS STRING) uf_proprietario, 
  SAFE_CAST(JSON_VALUE(content,'$.Cep') AS STRING) cep_proprietario,
  SAFE_CAST(JSON_VALUE(content,'$.Ufir') AS NUMERIC) valor_pagamento, 
  SAFE_CAST(JSON_VALUE(content,'$.VlPagto') AS NUMERIC) valor_pago, 
  IF(JSON_VALUE(content, '$.DtPagto') != '', SAFE_CAST(PARSE_DATE('%d/%m/%Y', JSON_VALUE(content,'$.DtPagto')) AS STRING), NULL) data_pagamento, -- data
  SAFE_CAST(JSON_VALUE(content,'$.Orgao') AS STRING) orgao, 
  SAFE_CAST(JSON_VALUE(content,'$.LocInf') AS STRING) local_autuacao, 
  SAFE_CAST(JSON_VALUE(content,'$.ProAutu') AS STRING) processo_defesa, -- Rever nome da coluna
  SAFE_CAST(JSON_VALUE(content,'$.NotifPen') AS STRING) penalidade_primeira_instancia, -- Rever nome  dacoluna
  SAFE_CAST(JSON_VALUE(content,'$.ProcRI') AS STRING) processo_troca_real_infrator,  -- Rever nome  dacoluna
FROM
  {{ source('infracao_staging','autuacoes_citran') }} as t

