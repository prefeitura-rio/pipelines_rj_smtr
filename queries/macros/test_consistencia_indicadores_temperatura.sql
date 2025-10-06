{% test consistencia_indicadores_temperatura(model) %}

WITH 
indicadores as (SELECT data, id_viagem, quantidade_pre_tr/home/duartejanaina/prefeitura_rio/pipelines_rj_smtr/queries/macros/test_consistencia_indicadores_temperatura.sqlatamento, quantidade_nula, quantidade_zero, quantidade_pos_tratamento, 
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_temperatura_transmitida_viagem.indicador_temperatura_transmitida_viagem') AS BOOL) AS indicador_temperatura_transmitida_viagem,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_temperatura_variacao_viagem.indicador_temperatura_variacao_viagem') AS BOOL) AS indicador_temperatura_variacao_viagem,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_temperatura_menor_igual_24.indicador_temperatura_menor_igual_24') AS BOOL) AS 
indicador_temperatura_menor_igual_24,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_temperatura_zero_viagem.indicador_temperatura_zero_viagem') AS BOOL) AS 
indicador_temperatura_zero_viagem,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_temperatura_pos_tratamento_descartada_viagem.indicador_temperatura_pos_tratamento_descartada_viagem') AS BOOL) AS indicador_temperatura_pos_tratamento_descartada_viagem,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_temperatura_nula_viagem.indicador_temperatura_nula_viagem') AS BOOL) AS indicador_temperatura_nula_viagem,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_validador.percentual_temperatura_nula_descartada') AS NUMERIC) AS percentual_temperatura_nula_descartada,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_validador.percentual_temperatura_pos_tratamento_descartada') AS NUMERIC) AS percentual_temperatura_pos_tratamento_descartada,
SAFE_CAST(JSON_VALUE(indicadores, '$.indicador_validador.percentual_temperatura_zero_descartada') AS NUMERIC) AS percentual_temperatura_zero_descartada,
 from `rj-smtr.subsidio_staging.aux_viagem_temperatura`),

/* countif(temperatura != 0) > 0 as indicador_temperatura_transmitida_viagem, */
/* Se q quantidade pré-tratamento for zero ou pós for zero, 
então não tem como ter temperatura transmitida como true */
indicador_temperatura_transmitida_viagem as
(select *
from indicadores
where (quantidade_pre_tratamento = 0 and indicador_temperatura_transmitida_viagem = true) or  
(quantidade_pos_tratamento = 0 and indicador_temperatura_transmitida_viagem = true)),

/*count(distinct case when temperatura != 0 then temperatura end) > 1 as indicador_temperatura_variacao_viagem*/
indicador_temperatura_variacao_viagem as
(select * from indicadores
where (
        (quantidade_pre_tratamento - quantidade_zero = 1 
         and indicador_temperatura_variacao_viagem = true) /*só 1 valor dif de 0 mas marcou true, ou seja, a temperatura se repetiu*/
     or (quantidade_pre_tratamento = quantidade_zero 
         and indicador_temperatura_variacao_viagem = true) /*Todas as temperaturas ficaram zero*/
)),
/* indicador_temperatura_pos_tratamento_descartada_viagem
  percentual_temperatura_pos_tratamento_descartada
            > 50 as indicador_temperatura_pos_tratamento_descartada_viagem,
            percentual_temperatura_zero_descartada
            = 100 as indicador_temperatura_zero_viagem,
            percentual_temperatura_nula_descartada
            = 100 as indicador_temperatura_nula_viagem*/
temperatura_perc as 
(SELECT *  from indicadores
WHERE (percentual_temperatura_pos_tratamento_descartada <= 50 and indicador_temperatura_pos_tratamento_descartada_viagem = true) OR
(percentual_temperatura_zero_descartada < 100 AND indicador_temperatura_zero_viagem = True ) or 
(percentual_temperatura_nula_descartada < 100 and indicador_temperatura_nula_viagem = True))

SELECT 
    i.data,
    i.id_viagem,
    CASE WHEN t.id_viagem IS NOT NULL THEN TRUE ELSE FALSE END AS inconsistencia_temp_transmitida,
    CASE WHEN v.id_viagem IS NOT NULL THEN TRUE ELSE FALSE END AS inconsistencia_temp_variacao,
    CASE WHEN p.id_viagem IS NOT NULL THEN TRUE ELSE FALSE END AS inconsistencia_percentuais

FROM indicadores i
LEFT JOIN indicador_temperatura_transmitida_viagem t USING (id_viagem, data)
LEFT JOIN indicador_temperatura_variacao_viagem v USING (id_viagem, data)
LEFT JOIN temperatura_perc p USING (id_viagem, data)
WHERE t.id_viagem IS NOT NULL 
   OR v.id_viagem IS NOT NULL 
   OR p.id_viagem IS NOT NULL
{%- endtest %}










