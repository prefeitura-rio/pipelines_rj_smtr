# Relatório Semanal de Auditoria - Semana 06/02/2026

## 📊 Resumo Executivo

**Data da Análise:** 06 de Fevereiro de 2026
**Commits Analisados:** 27 commits (depois da atualização anterior de 12/01/2026)
**Commit Base:** `2d89e11b2` → `8e3704fbb`
**Status:** ✅ Repositório atualizado

---

## 🚨 MUDANÇAS CRÍTICAS IDENTIFICADAS

### 1. AUMENTO DA TARIFA DE INTEGRAÇÃO: R$ 4,70 → R$ 5,00 ⚠️

**Commit:** `22d4617d5` - PR #1162
**Data de Implementação:** 04/01/2026 (via DECRETO RIO Nº 57473/2025)
**Impacto:** **ALTO** - Aumento de 6,38% na tarifa paga por integração

#### O que mudou:

**Antes:**
```yaml
# Valor hardcoded no código
valor_integracao: 4.7 (R$ 4,70)
```

**Depois:**
```sql
-- Nova tabela: tarifa_publica.sql
-- Histórico completo de tarifas:
2023-01-07: R$ 4,30 (DECRETO RIO 51914/2023)
2025-01-05: R$ 4,70 (DECRETO RIO 55631/2025)
2026-01-04: R$ 5,00 (DECRETO RIO 57473/2025) ← NOVO
```

#### Estrutura da Mudança:

1. **Novo Modelo:** `tarifa_publica.sql` (tabela permanente)
   - Histórico de todas as tarifas desde 2023
   - Atualização via DECRETOS (não mais hardcoded)
   - Referenciada por data de vigência

2. **Scripts PySpark Atualizados:**
   - `aux_calculo_integracao.py`: Agora busca tarifa pela data da transação
   - `aux_transacao_filtro_integracao_calculada.sql`: JOIN com `tarifa_publica`

3. **Matrizes de Integração:**
   - `aux_matriz_integracao_modo.sql`: Usa tarifa dinâmica
   - `aux_matriz_transferencia.sql`: Nova tabela de transferências

#### Impacto Financeiro:

**Cenário Hipotético:**
- 100.000 integrações/dia
- Valor antigo: 100.000 × R$ 4,70 = **R$ 470.000/dia**
- Valor novo: 100.000 × R$ 5,00 = **R$ 500.000/dia**
- **Aumento:** R$ 30.000/dia (+6,38%)

**Interpretação Jurídica:**
- ✅ Positivo: Aumento de tarifa beneficia operadoras
- ⚠️ Mas: AUMENTA CUSTO DO SUBSÍDIO para a Prefeitura
- ⚠️ Possível contrapartida: Novas restrições podem vir

---

### 2. PRORROGAÇÃO DE PRAZO DE VISTORIA ATÉ 31/01/2026

**Commit:** `721061f82` - PR #1183
**Base Legal:** RESOLUÇÃO SMTR Nº 3894 (29/12/2025)
**Impacto:** **MÉDIO** - Redução temporária de glosas por vistoria

#### O que mudou:

**Modelo:** `veiculo_dia.sql`

```sql
-- NOVA EXCEÇÃO:
when
    date(data) between ('2026-01-01') and ('2026-01-31')
    and ano_ultima_vistoria >= extract(year from date(data)) - 2
then true
-- RESOLUÇÃO SMTR Nº 3894 DE 29 DE DEZEMBRO DE 2025
-- que altera o prazo final de vistoria para 31 de janeiro de 2026
```

**Regra Antiga (fora do período):**
- Prazo de vistoria: 1 ano
- Exceção: 15 dias para veículos novos

**Regra Temporária (JAN/2026):**
- Prazo estendido: 2 anos (veículos de 2024+)
- Período: 01/01/2026 a 31/01/2026

**Interpretação:**
- ✅ Menos glosas por "não vistoriado" em JAN/2026
- ⚠️ Mas prazo é TEMPORÁRIO (apenas 1 mês)
- ⚠️ Possível preparação para exigência mais rigorosa após 01/02/2026

---

### 3. REATIVAÇÃO DE INTEGRAÇÃO INVÁLIDA

**Commit:** `d2acbfdfc` - PR #1182
**Impacto:** **ALTO** - Novo tipo de glosa por falha de integração

#### O que é:

**Novo Modelo:** `integracao_invalida.sql` (599 linhas!)
- Classifica falhas de integração entre viagens
- Cria novo tipo de viagem inválida
- **Glosa:** Viagens com integração falha não são pagas

#### Lógica Implementada:

```python
# aux_calculo_integracao.py (PySpark)
# Detecta falhas como:
- Tempo de integração > limite permitido
- Falta de registro na matriz de integração
- Erro na identificação do modo de transporte
```

**Interpretação Jurídica:**
- ❌ Mais uma forma de glosa
- ❌ Critérios técnicos podem ser questionados
- ❌ Complexidade da lógica (599 linhas) dificulta defesa

---

### 4. CORREÇÕES TÉCNICAS DIVERSAS

#### a) Remoção de Acréscimo de 4% no RioCard

**Commit:** `635c0673d` - PR #1185
**Mudança:** Removido acréscimo de 4% nas transações RioCard

**Interpretação:**
- ✅ Positivo para passageiros (tarifa reduzida)
- ⚠️ Mas pode indicar renegociação contratual

#### b) Correção de Teste de Temperatura

**Commit:** `69347ad81` - PR #1189
**Mudança:** Retirado o "dia posterior" do teste de completude

**Problema Anterior:**
- Teste validava temperatura do dia D+1 indevidamente
- Gerava falsas falhas

**Interpretação:**
- ✅ Correção justa
- ⚠️ Mas quantas glosas indevidas foram aplicadas antes?

#### c) Exceção no Limite de Viagens

**Commit:** `0a46cdadc` - PR #1195
**Mudança:** `viagens_remuneradas_v2` - Adiciona exceção para serviços específicos

**Serviços Beneficiados:**
- 161, LECD110, 583, 584, 109 (códigos de serviços)
- Possivelmente linhas com problemas operacionais estruturais

**Interpretação:**
- ✅ Reconhecimento de impossibilidade técnica
- ⚠️ Caso a caso (não é regra geral)

#### d) Ponto Facultativo Revertido

**Commit:** `ed92813e6` - PR #1180
**Mudança:** Reverte ponto facultativo do dia 31/10

**Interpretação:**
- ❌ 31/10 NÃO era ponto facultativo
- ⚠️ Erro de classificação anterior corrigido

---

## 📈 ANÁLISE DE TENDÊNCIAS

### Padrão Identificado:

1. **Janeiro/2026:** Mês de "ajustes técnicos"
   - Correção de testes de temperatura
   - Ajuste de tarifas (integração)
   - Prorrogação de prazos (vistoria)

2. **Fevereiro/2026 (previsão):**
   - ⚠️ Possível fim das "concessões" de JAN
   - ⚠️ Novas restrições podem surgir após 31/01
   - ⚠️ V99 continua como "bomba-relógio"

### Tendências Positivas:
- ✅ Tarifa de integração aumentou
- ✅ Prazo de vistoria prorrogado (temporariamente)
- ✅ Acréscimo RioCard removido

### Tendências Negativas:
- ❌ Nova glosa por "integração inválida"
- ❌ Complexidade do código aumentando
- ❌ Correções indicam erros anteriores (glosas indevidas?)

---

## 🎯 RECOMENDAÇÕS PARA A SEMANA

### Curto Prazo:

1. **Calcular Impacto da Tarifa de Integração**
   ```sql
   SELECT
     COUNT(*) * 5.0 as valor_novo,
     COUNT(*) * 4.7 as valor_antigo,
     (COUNT(*) * 0.3) as diferenca
   FROM transacao
   WHERE produto = 'Integração'
     AND data BETWEEN '2026-01-04' AND CURRENT_DATE()
   ```

2. **Verificar Glosas de Temperatura Revertidas**
   - Quantas viagens deixaram de ser glosadas após PR #1189?
   - Valor correspondente?

3. **Monitorar Integracao Inválida**
   - Quantas viagens estão sendo glosadas por esse novo critério?
   - Critérios técnicos estão claros?

### Médio Prazo:

1. **Preparar para 01/02/2026**
   - Prazo de vistoria volta ao normal (1 ano)
   - Possível aumento de glosas "não vistoriado"

2. **Acompanhar V99**
   - Verificar se Prefeitura planeja ativá-la
   - Discutir com juridicamente

---

## 📋 ARQUIVOS ALTERADOS (Principais)

| Arquivo | Tipo | Impacto |
|---------|------|----------|
| `tarifa_publica.sql` | NOVO | ⚠️ Histórico de tarifas |
| `integracao_invalida.sql` | NOVO | ⚠️ Nova glosa (599 linhas) |
| `veiculo_dia.sql` | ALTERADO | ⚠️ Prorrogação vistoria |
| `aux_calculo_integracao.py` | ALTERADO | ⚠️ Tarifa dinâmica |
| `aux_matriz_transferencia.sql` | NOVO | Tarifa dinâmica |
| `viagens_remuneradas_v2.sql` | ALTERADO | Exceções serviços |

---

## 🔍 PRÓXIMOS PASSOS

1. ✅ Repositório atualizado (commit `2068fc362`)
2. ⏳ Aguardar próximos commits upstream
3. ⏳ Monitorar ativação de V99
4. ⏳ Verificar se novas restrições surgem após 31/01

---

**Relatório gerado:** 06/02/2026
**Próxima atualização:** 13/02/2026 (semanal)
