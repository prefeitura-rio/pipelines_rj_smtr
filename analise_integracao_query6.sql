-- QUERY 6: RESUMO GERAL (CORRIGIDA)
SELECT 
    COUNT(DISTINCT id_integracao) as total_integracoes,
    COUNT(CASE WHEN sequencia_integracao = 1 THEN 1 END) as total_primeiras_pernas,
    COUNT(CASE WHEN sequencia_integracao > 1 AND valor_transacao = 0 THEN 1 END) as integracoes_ok_2a_perna_gratis,
    COUNT(CASE WHEN sequencia_integracao > 1 AND valor_transacao > 0 THEN 1 END) as cobranca_duplicada,
    ROUND(COUNT(CASE WHEN sequencia_integracao > 1 AND valor_transacao > 0 THEN 1 END) * 100.0 / COUNT(CASE WHEN sequencia_integracao > 1 THEN 1 END), 2) as percentual_cobranca_duplicada
FROM `rj-smtr.bilhetagem.integracao`
WHERE data BETWEEN DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY) AND CURRENT_DATE('America/Sao_Paulo');
