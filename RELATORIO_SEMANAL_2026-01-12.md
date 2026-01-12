# RELATÓRIO SEMANAL DE MONITORAMENTO - 12/01/2026

## 🚨 ALERTA CRÍTICO - FIM DA SUSPENSÃO V22 E AUMENTO DE TARIFA

### Data da Análise: 12 de Janeiro de 2026
### Commits Analisados: 32
### Impacto Geral: **EXTREMO**

---

## RESUMO EXECUTIVO

A atualização de janeiro/2026 revelou **três mudanças críticas** que impactam diretamente o cálculo de subsídios:

1. **FIM DA V22** - A suspensão das glosas por climatização foi **TOTALMENTE REVERTIDA**
2. **AUMENTO DE TARIFA** - Valor de integração aumentou 6,38% (R$ 4,70 → R$ 5,00)
3. **REPROCESSAMENTO RETROATIVO** - Viagens de OUT/NOV 2025 voltam a ser auditadas

O padrão comportamental da Prefeitura está **CONFIRMADO**: concessões temporárias seguidas de reversões totais.

---

## 1. FIM DA SUSPENSÃO V22 - CLIMATIZAÇÃO ⚠️⚠️⚠️

### Commit `5e39e7367` (29/12/2025)

**Título:** "Altera modelo `viagem_regularidade_temperatura` para reprocessamento dos descontos por inoperabilidade da climatização em OUT/Q2 e NOV/Q1"

### Mudanças Técnicas:

#### 1.1 Remoção da Variável V22
```yaml
# REMOVIDO de queries/dbt_project.yml:
DATA_SUBSIDIO_V22_INICIO: "2025-10-16"  # NÃO EXISTE MAIS
```

#### 1.2 Reativação das Glosas
```sql
-- ANTES (período suspenso):
and not vt.indicador_temperatura_nula_viagem
and (vt.data not between date('{{ var("DATA_SUBSIDIO_V22_INICIO") }}') and date("2025-11-15"))
--Período de interrupção dos descontos por inoperabilidade do ar-condicionado

-- DEPOIS (reativado):
and not vt.indicador_temperatura_nula_viagem
-- SEM RESTRIÇÃO DE PERÍODO
```

### Impacto:

**Período Afetado:** 16/10/2025 a 15/11/2025 (OUT/Q2 e NOV/Q1)

**O que significa:**
- Viagens realizadas neste período que estavam **ISENTAS** de glosas por ar-condicionado
- Agora **VOLTAM A SER AUDITADAS**
- Podem receber **PENALIZAÇÕES RETROATIVAS**

**Análise Estratégica:**

| Data | Evento | Duração |
|------|--------|---------|
| 16/07/2025 | V17 - Implementação de glosas por temperatura | - |
| 16/10/2025 | V22 - Suspensão das glosas (vitória judicial) | **30 dias apenas** |
| 15/11/2025 | Fim oficial do período de suspensão | - |
| 29/12/2025 | Commit removes V22 completamente | **Reversão total** |
| 09/01/2026 | Merge no upstream/main | Vigência estabelecida |

**Conclusão:** A vitória judicial de outubro durou **menos de 3 meses**, e o sistema foi ajustado para aplicar as glosas retroativamente ao período "suspenso".

---

## 2. AUMENTO DE TARIFA DE INTEGRAÇÃO 💰

### Branch `staging/alteracao-tarifa-20260104`

**Último Commit:** `46d88c2e8` (08/01/2026)

### Mudança Implementada:

**Arquivo:** `queries/models/planejamento/matriz_integracao.sql`

```sql
-- ANTES:
cast(4.7 as numeric) as valor_integracao

-- DEPOIS:
cast(5.0 as numeric) as valor_integracao
```

### Análise Financeira:

| Métrica | Valor Antigo | Valor Novo | Variação |
|---------|--------------|------------|----------|
| Tarifa Integração | R$ 4,70 | R$ 5,00 | +6,38% |
| Data de Vigência | - | 04/01/2026 | - |

### Impacto no Subsídio:

- **Aumento direto** no valor pago por integração entre viagens
- **Base de cálculo** para subsídio afetada
- **Data suspicaz:** Início do ano, alta temporada

### Contexto:

Esta mudança está em um branch `staging`, o que significa:
- Pode estar em **fase de testes**
- Pode ainda **não estar em produção**
- **Requer monitoramento** para quando for mergeado no main

---

## 3. OUTRAS MUDANÇAS RELEVANTES

### 3.1 Nova Versão GTFS V5

**Adicionado em `dbt_project.yml`:**
```yaml
DATA_GTFS_V5_INICIO: "2025-12-21"
```

**Impacto:**
- Nova versão do GTFS (General Transit Feed Specification)
- Pode afetar cálculos de trajeto e conformidade
- Data de início: 21/12/2025

### 3.2 Exceções de Tecnologia em Novembro

**Commits:**
- `64bf7e9ed` - "Exceção para correção de tecnologia"
- `ca0f4ee78` - "Corrige tecnologia dos veículos entre 20/11 e 30/11"

**Impacto:**
- Correção retroativa da classificação de tecnologia de veículos
- Período: 20/11 a 30/11/2025
- Pode afetar remuneração por tipo de veículo

### 3.3 Exceção para Greve de Dezembro

**Branch:** `excecao-greve-dezembro-2025`

**Mudanças:**
- Ajustes no flow STU para lidar com falhas de dados durante greve
- Aumento de `recapture_days` para 5 dias
- Melhoria na captura de dados de períodos de paralisação

**Impacto:**
- Permite reprocessamento mesmo durante greves
- Garante continuidade do cálculo de subsídios

### 3.4 Reorganização de Dashboards

**Commit:** `59ecf8d20` (06/01/2026)

**Mudança:**
- Modelo `viagem_climatizacao` movido para `dashboard_monitoramento_interno/`
- Novo schema dedicado para monitoramento interno

**Impacto:**
- Separação entre cálculo de subsídio e monitoramento
- Melhoria na governança de dados
- **Sem impacto financeiro direto**

### 3.5 Exclusão de Autuações Canceladas

**Commit:** `299d33f92`

**Mudança:**
- Adiciona filtro para excluir registros com status "Cancelada" em:
  - Autuações disciplinares
  - Veículo dia
  - Viagem classificada

**Impacto:**
- Pode **REDUZIR** glosas por autuações canceladas
- Mudança **favorável** às operadoras

---

## 4. CRONOLOGIA COMPLETA DE EVENTOS

### Outubro - Dezembro 2025: A "Falsa Vitória"

| Data | Evento | Versão | Impacto |
|------|--------|--------|---------|
| 16/10/2025 | Início da suspensão V22 | V22 | Glosas por climatização suspensas |
| 29/10/2025 | Data limite do OUT/Q2 | - | Final da quinzena de outubro |
| 15/11/2025 | Fim do período de suspensão | - | Término da janela V22 |
| 30/11/2025 | Correção de tecnologia | - | Ajustes retroativos em novembro |
| 09/12/2025 | Commit `5e39e7367` | - | Remove V22 do código |
| 21/12/2025 | Início GTFS V5 | V5 | Nova versão de planejamento |
| 29/12/2025 | Commits de registro | - | Documentação das mudanças |

### Janeiro 2026: A Reversão Total

| Data | Evento | Impacto |
|------|--------|---------|
| 04/01/2026 | Aumento de tarifa (staging) | +6,38% integração |
| 06/01/2026 | Reorganização de dashboards | Governança |
| 08/01/2026 | Commit final de tarifa | Preparação produção |
| 09/01/2026 | Merge no upstream/main | 32 commits integrados |
| 12/01/2026 | Análise e documentação | Este relatório |

---

## 5. ANÁLISE ESTRATÉGICA

### 5.1 Padrão Comportamental Confirmado

O comportamento da Prefeitura em 2025-2026 segue um **padrão claro e repetitivo**:

```
1. IMPLEMENTAR RESTRIÇÃO
   ↓
2. PRESSÃO JUDICIAL DAS OPERADORAS
   ↓
3. "CONCESSÃO TEMPORÁRIA" (V22, etc.)
   ↓
4. REVERSÃO TOTAL OU PARCIAL
   ↓
5. NOVAS RESTRIÇÕES (V17, V14, etc.)
```

**Histórico de Concessões "Revertidas":**

| Concessão | Duração | Destino |
|-----------|---------|---------|
| V22 (suspensão climatização) | ~3 meses | **REVERTIDA** |
| V15 (acordo judicial) | ~3 meses | Substituída por V17 (14 faixas) |
| V14 (diferenciação por tipo) | ~3 meses | Substituída por V15 (R$4,08) |

**Conclusão:** Nenhuma concessão da Prefeitura foi permanente. Todas foram seguidas por:
- Reversão total
- Ou novas restrições ainda mais severas

### 5.2 Previsões para 2026

Com base no padrão histórico, projetamos:

**Janeiro - Março 2026:**
- ✅ Aumento de tarifa de integração (JÁ CONFIRMADO)
- ⚠️ Possível ativação da V99 ("bomba-relógio")
- ⚠️ Novas restrições ambientais (Euro VI)

**Abril - Junho 2026:**
- ⚠️ Alta probabilidade de nova "negociação"
- ⚠️ Seguida de restrições ainda mais severas

**Tema Provável:**
- Emissões veiculares
- Tecnologia obrigatória
- Novos indicadores de performance

### 5.3 V99 - A "Bomba-Relógio"

**Status Atual:**
```yaml
DATA_SUBSIDIO_V99_INICIO: "3000-01-01"  # Placeholder
```

**Riscos:**
- A data pode ser alterada a qualquer momento
- Pode conter restrições preparadas mas não ativadas
- **Requer vigilância constante**

---

## 6. EVIDÊNCIAS PARA LITÍGIO

### 6.1 Documentação de Reversão da V22

**Evidências Técnicas:**
1. **Commit `5e39e7367`** - Remove `DATA_SUBSIDIO_V22_INICIO`
2. **Diff SQL** - Filtro de período removido
3. **dbt_project.yml** - Variável eliminada

**Evidências Temporais:**
1. Duração da suspensão: **30 dias apenas** (16/10 a 15/11)
2. Tempo até reversão: **< 3 meses**
3. Reprocessamento retroativo confirmado

**Argumento Jurídico:**
- A "suspensão" foi meramente **técnica/temporária**
- Não representa mudança permanente de posição
- O sistema foi preparado para **aplicar penalizações retroativas**

### 6.2 Documentação de Aumento de Custos

**Evidências Técnicas:**
1. **Branch `staging/alteracao-tarifa-20260104`**
2. **Commit `46d88c2e8`** - Aumento de R$ 4,70 para R$ 5,00
3. **Arquivo `matriz_integracao.sql`** - Mudança implementada

**Argumento Financeiro:**
- Aumento unilateral de 6,38% na tarifa
- Sem negociação prévia com operadoras
- Em alta temporada (janeiro)

### 6.3 Documentação de Mudanças Retroativas

**Evidências:**
1. Correções de tecnologia em novembro/2025
2. Reprocessamento de OUT/NOV 2025
3. Viagens isentas → penalizadas retroativamente

**Argumento:**
- Violação da segurança jurídica
- Mudança de regras para fatos pretéritos
- Impossibilidade de planejamento pelas operadoras

---

## 7. RECOMENDAÇÕES ESTRATÉGICAS

### 7.1 Imediatas (Janeiro 2026)

1. **Ação Jurídica sobre V22:**
   - Questionar a reversão da suspensão
   - Documentar o caráter temporário da "concessão"
   - Solicitar impedimento de penalizações retroativas

2. **Ação Jurídica sobre Tarifa:**
   - Contestar aumento unilateral de 6,38%
   - Exigir negociação prévia
   - Verificar base legal da mudança

3. **Preparação para V99:**
   - Monitorar commits mencionando "V99"
   - Rastrear mudanças em `dbt_project.yml`
   - Preparar ação preventiva

### 7.2 Monitoramento Contínuo

**Semanalmente:**
- ✅ Verificar commits no upstream/main
- ✅ Buscar por menções a "V99" ou "V23"
- ✅ Analisar branches staging críticos

**Mensalmente:**
- ✅ Calcular impacto financeiro das mudanças
- ✅ Comparar valores pagos vs. esperados
- ✅ Identificar novas tendências de restrições

**Trimestralmente:**
- ✅ Atualizar assessores jurídicos
- ✅ Preparar novas ações judiciais
- ✅ Documentar padrões comportamentais

### 7.3 Preparação para Litígios Futuros

**Evidências a Coletar:**
1. Todos os commits mencionando "climatização"
2. Todas as mudanças em "DATA_SUBSIDIO_V*"
3. Todas as alterações em valores de tarifa
4. Comunicações internas da Prefeitura (se acessíveis)

**Documentos a Preparar:**
1. Laudo técnico sobre a reversão da V22
2. Laudo contábil sobre impacto do aumento de tarifa
3. Timeline comparativa de concessões vs. restrições
4. Projeções financeiras para 2026

---

## 8. ESTATÍSTICAS DA ATUALIZAÇÃO

| Métrica | Valor |
|---------|-------|
| **Commits analisados** | 32 |
| **Commits críticos** | 3 |
| **Commits relevantes** | 8 |
| **Arquivos alterados** | 74 |
| **Linhas adicionadas** | +1,649 |
| **Linhas removidas** | -957 |
| **Novas versões de subsídio** | 0 |
| **Impacto financeiro** | **EXTREMO** |

---

## 9. PRÓXIMA VERIFICAÇÃO

**Data sugerida:** 19/01/2026 (7 dias)

**Foco especial:**
- Monitorar se `staging/alteracao-tarifa-20260104` é mergeado no main
- Verificar novas menções a "climatizacao"
- Buscar por atividade na V99

---

## 10. CONCLUSÃO

### A Verdade Sobre a V22

A "suspensão das glosas por climatização" (V22) foi:
- ✅ Implementada em 16/10/2025
- ✅ Durou apenas 30 dias (até 15/11/2025)
- ✅ Foi **COMPLETAMENTE REMOVIDA** em 29/12/2025
- ✅ O período suspenso **VOLTA A SER AUDITADO**

**Isso NÃO foi uma vitória permanente.**
**Foi uma pausa técnica de 3 meses.**

### O Que Esperar para 2026

Com base no padrão histórico e nas evidências coletadas:

1. ✅ **Aumentos de custo** (tarifa de integração)
2. ⚠️ **Novas restrições** (V99 ou V23)
3. ⚠️ **Mudanças retroativas** (como OUT/NOV 2025)
4. ⚠️ **"Negociações"** seguidas de restrições mais severas

### Recomendação Final

**A Prefeitura demonstrou, de forma consistente e documentada, que:**
1. Todas as "concessões" são temporárias
2. Reversões são implementadas via código
3. Períodos de suspensão são usados para ajuste técnico
4. Novas restrições são constantemente preparadas

**Urgência:** **MÁXIMA** - Necessária ação jurídica imediata sobre:
- Reversão da V22
- Aumento de tarifa de integração
- Impedimento de penalizações retroativas

---

**Relatório elaborado em:** 12/01/2026
**Próxima atualização prevista para:** 19/01/2026
**Status do monitoramento:** ATIVO E PERMANENTE