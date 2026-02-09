-- ===================================================================
-- ANÁLISE DE INTEGRAÇÕES - VERSÃO FINAL CORRIGIDA
-- Todas as queries com filtro de partição em TODOS os níveis
-- ===================================================================

-- ===================================================================
-- QUERY 1: COMBINAÇÕES DE LINHAS MAIS POPULARES
-- ===================================================================
SELECT 
    a.servico_jae as linha_origem,
    b.servico_jae as linha_destino,
    COUNT(DISTINCT a.id_integracao) as quantidade_integracoes,
    ROUND(AVG(TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE)), 2) as tempo_medio_minutos,
    ROUND(MIN(TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE)), 2) as tempo_min,
    ROUND(MAX(TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE)), 2) as tempo_max
FROM `rj-smtr.bilhetagem.integracao` a
JOIN `rj-smtr.bilhetagem.integracao` b
    ON a.id_integracao = b.id_integracao
    AND a.sequencia_integracao = b.sequencia_integracao - 1
    AND b.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 DAY)  -- Filtro aqui também!
WHERE a.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 DAY)  -- Filtro de partição
    AND a.servico_jae IS NOT NULL
    AND b.servico_jae IS NOT NULL
    AND a.servico_jae != b.servico_jae  -- Linhas diferentes
GROUP BY linha_origem, linha_destino
ORDER BY quantidade_integracoes DESC
LIMIT 50;

-- ===================================================================
-- QUERY 2: VERIFICAR SE COMBINAÇÕES POPULARES ESTÃO NA MATRIZ
-- ===================================================================
WITH matriz_ativa AS (
    SELECT *
    FROM `rj-smtr.planejamento.matriz_integracao`
    WHERE data_inicio <= CURRENT_DATE('America/Sao_Paulo')
        AND (data_fim >= CURRENT_DATE('America/Sao_Paulo') OR data_fim IS NULL)
        AND indicador_integracao = TRUE
),
combinacoes_populares AS (
    SELECT 
        a.servico_jae as linha_origem,
        b.servico_jae as linha_destino,
        COUNT(DISTINCT a.id_integracao) as quantidade
    FROM `rj-smtr.bilhetagem.integracao` a
    JOIN `rj-smtr.bilhetagem.integracao` b
        ON a.id_integracao = b.id_integracao
        AND a.sequencia_integracao = b.sequencia_integracao - 1
        AND b.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)
    WHERE a.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)  -- Filtro de partição
        AND a.servico_jae IS NOT NULL
        AND b.servico_jae IS NOT NULL
        AND a.servico_jae != b.servico_jae
    GROUP BY linha_origem, linha_destino
)
SELECT 
    cp.linha_origem,
    cp.linha_destino,
    cp.quantidade,
    CASE 
        WHEN ma.servico_origem IS NOT NULL THEN 'NA MATRIZ'
        ELSE 'FORA DA MATRIZ - PROBLEMA!'
    END as status_matriz
FROM combinacoes_populares cp
LEFT JOIN matriz_ativa ma
    ON cp.linha_origem = ma.servico_origem
    AND cp.linha_destino = ma.servico_destino
ORDER BY cp.quantidade DESC
LIMIT 50;

-- ===================================================================
-- QUERY 3: TEMPOS DE INTEGRAÇÃO VS LIMITE DA MATRIZ
-- ===================================================================
SELECT 
    a.servico_jae as linha_origem,
    b.servico_jae as linha_destino,
    COUNT(*) as total_integracoes,
    ROUND(AVG(TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE)), 2) as tempo_medio_real,
    ROUND(AVG(ma.tempo_integracao_minutos), 2) as tempo_medio_limite,
    COUNT(CASE WHEN TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE) > ma.tempo_integracao_minutos THEN 1 END) as quebras_limite,
    ROUND(COUNT(CASE WHEN TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE) > ma.tempo_integracao_minutos THEN 1 END) * 100.0 / COUNT(*), 2) as percentual_falha_tempo
FROM `rj-smtr.bilhetagem.integracao` a
JOIN `rj-smtr.bilhetagem.integracao` b
    ON a.id_integracao = b.id_integracao
    AND a.sequencia_integracao = b.sequencia_integracao - 1
    AND b.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)
LEFT JOIN `rj-smtr.planejamento.matriz_integracao` ma
    ON a.servico_jae = ma.servico_origem
    AND b.servico_jae = ma.servico_destino
    AND ma.data_inicio <= a.data
    AND (ma.data_fim >= a.data OR ma.data_fim IS NULL)
WHERE a.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)  -- Filtro de partição
    AND a.servico_jae IS NOT NULL
    AND b.servico_jae IS NOT NULL
    AND a.servico_jae != b.servico_jae
    AND ma.tempo_integracao_minutos IS NOT NULL
GROUP BY linha_origem, linha_destino
ORDER BY percentual_falha_tempo DESC
LIMIT 50;

-- ===================================================================
-- QUERY 4: INTEGRAÇÕES MESMA LINHA (IDA/VOLTA)
-- ===================================================================
SELECT 
    a.servico_jae as linha,
    COUNT(DISTINCT a.id_integracao) as tentativas_mesma_linha,
    ROUND(AVG(TIMESTAMP_DIFF(b.datetime_transacao, a.datetime_transacao, MINUTE)), 2) as tempo_medio
FROM `rj-smtr.bilhetagem.integracao` a
JOIN `rj-smtr.bilhetagem.integracao` b
    ON a.id_integracao = b.id_integracao
    AND a.sequencia_integracao = b.sequencia_integracao - 1
    AND b.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 DAY)
WHERE a.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 DAY)  -- Filtro de partição
    AND a.servico_jae = b.servico_jae  -- MESMA LINHA
    AND a.id_servico_jae != b.id_servico_jae  -- Mas código diferente
GROUP BY linha
HAVING COUNT(DISTINCT a.id_integracao) > 10  -- Pelo menos 10 tentativas
ORDER BY tentativas_mesma_linha DESC
LIMIT 50;

-- ===================================================================
-- QUERY 5: COBRANÇA DUPLICADA - A MAIS IMPORTANTE! ⭐
-- ===================================================================
SELECT 
    a.servico_jae as linha_origem,
    b.servico_jae as linha_destino,
    COUNT(*) as quantidade_cobracao_dupla,
    ROUND(AVG(b.valor_transacao), 2) as valor_medio_2a_perna
FROM `rj-smtr.bilhetagem.integracao` a
JOIN `rj-smtr.bilhetagem.integracao` b
    ON a.id_integracao = b.id_integracao
    AND a.sequencia_integracao = b.sequencia_integracao - 1
    AND b.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)
WHERE a.data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)  -- Filtro de partição
    AND a.servico_jae IS NOT NULL
    AND b.servico_jae IS NOT NULL
    AND b.valor_transacao > 0  -- COBROU na 2ª perna = DUPLICADA!
GROUP BY linha_origem, linha_destino
ORDER BY quantidade_cobracao_dupla DESC
LIMIT 50;

-- ===================================================================
-- QUERY 6: RESUMO GERAL - TODAS AS INTEGRAÇÕES
-- ===================================================================
SELECT 
    COUNT(DISTINCT id_integracao) as total_integracoes,
    COUNT(CASE WHEN sequencia_integracao = 1 THEN 1 END) as total_primeiras_pernas,
    COUNT(CASE WHEN sequencia_integracao > 1 AND valor_transacao = 0 THEN 1 END) as integracoes_ok_2a_perna_gratis,
    COUNT(CASE WHEN sequencia_integracao > 1 AND valor_transacao > 0 THEN 1 END) as cobranca_duplicada,
    ROUND(COUNT(CASE WHEN sequencia_integracao > 1 AND valor_transacao > 0 THEN 1 END) * 100.0 / COUNT(CASE WHEN sequencia_integracao > 1 THEN 1 END), 2) as percentual_cobranca_duplicada
FROM `rj-smtr.bilhetagem.integracao`
WHERE data >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY);

