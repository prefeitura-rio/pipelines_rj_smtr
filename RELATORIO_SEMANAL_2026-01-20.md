# RELATÓRIO SEMANAL DE MONITORAMENTO - 20/01/2026

## 🚨 ALERTA CRÍTICO - AUMENTO DE TARIFA CONFIRMADO E MUDANÇAS EM MODELOS CENTRAIS

### Data da Análise: 20 de Janeiro de 2026
### Commits Analisados: 10 (desde 12/01/2026)
### Impacto Geral: **ALTO**

---

## RESUMO EXECUTIVO

A atualização semanal de janeiro/2026 revelou **mudanças significativas** que impactam diretamente o cálculo de subsídios:

1. **AUMENTO DE TARIFA MERGEADO** - Valor de integração aumentou 6,38% (R$ 4,70 → R$ 5,00)
2. **ALTERAÇÕES EM MODELOS CRÍTICOS** - `viagem_transacao`, `valor_tipo_penalidade`, `temperatura`
3. **REVERSÃO DE PONTO FACULTATIVO** - 31/10/2025: adicionado e removido no mesmo dia
4. **MELHORIAS EM TESTES** - GTFS, temperatura e completude

O padrão comportamental da Prefeitura se **CONFIRMA**: aumentos de custo implementados rapidamente após o início do ano.

---

## 1. AUMENTO DE TARIFA DE INTEGRAÇÃO 💰💰💰

### Commit `22d4617d5` - 13/01/2026 (MERGEADO)

**Título:** "Bilhetagem - Altera tarifa para 5 reais a partir de `2025-01-04`"
**PR:** #1162
**Base Legal:** DECRETO RIO Nº 57473 DE 29 DE DEZEMBRO DE 2025

### Mudanças Implementadas:

#### 1.1 Nova Tabela `tarifa_publica.sql`

**Localização:** `queries/models/planejamento/tarifa_publica.sql`

**Estrutura:**
```sql
select
    date(a.data_inicio) as data_inicio,
    date_sub(
        date(lead(a.data_inicio) over (order by a.data_inicio)), interval 1 day
    ) as data_fim,
    cast(a.valor_tarifa as numeric) as valor_tarifa,
    a.legislacao
from unnest([
    struct("2023-01-07" as data_inicio, 4.3 as valor_tarifa,
           "DECRETO RIO Nº 51914 DE 2 DE JANEIRO DE 2023" as legislacao),
    struct("2025-01-05" as data_inicio, 4.7 as valor_tarifa,
           "DECRETO RIO Nº 55631 DE 1º DE JANEIRO DE 2025" as legislacao),
    struct("2026-01-04" as data_inicio, 5.0 as valor_tarifa,
           "DECRETO RIO Nº 57473 DE 29 DE DEZEMBRO DE 2025" as legislacao)
]) as a
```

#### 1.2 Histórico de Tarifas

| Data Início | Valor | Decreto | Variação |
|-------------|-------|---------|----------|
| 2023-01-07 | R$ 4,30 | DECRETO RIO Nº 51914/2023 | - |
| 2025-01-05 | R$ 4,70 | DECRETO RIO Nº 55631/2025 | +9,30% |
| **2026-01-04** | **R$ 5,00** | **DECRETO RIO Nº 57473/2025** | **+6,38%** |

#### 1.3 Alteração em `matriz_integracao.sql`

**Antes:**
```sql
from {{ source("source_smtr", "matriz_transferencia") }}
...
cast(4.7 as numeric) as valor_integracao
```

**Depois:**
```sql
from {{ ref("aux_matriz_transferencia") }}
...
t.valor_tarifa as valor_integracao
```

**Mudança:** Valor hardcoded (4.7) substituído por join dinâmico com tabela `tarifa_publica`.

#### 1.4 Novo Modelo `aux_matriz_transferencia.sql`

**Função:** Cruzamento da matriz de transferência com a tabela de tarifas públicas

**Lógica:**
```sql
select
    case
        when m.data_inicio >= t.data_inicio then m.data_inicio
        when m.data_inicio < t.data_inicio then t.data_inicio
    end as data_inicio,
    ...
    t.valor_tarifa as valor_integracao,
    ...
from matriz m
left join {{ ref("tarifa_publica") }} t
    on (t.data_fim >= m.data_inicio or t.data_fim is null)
    and (m.data_fim >= t.data_fim or m.data_fim is null)
```

**Resultado:** Sistema agora suporta múltiplas tarifas ao longo do tempo de forma automatizada.

### Impacto Financeiro:

**Aumento direto** no valor pago por integração entre viagens:
- **+6,38%** sobre tarifa anterior (R$ 4,70)
- **Base de cálculo** para subsídio afetada
- **Data suspicaz:** 04/01/2026 (alta temporada)

**Estimativa de impacto:**
- Depende do volume de integrações diárias
- Se X integrações/dia → aumento de (X × R$ 0,30)/dia
- **Acima de 1 milhão de integrações/mês = +R$ 300.000/mês apenas em integrações**

### Cronologia:

| Data | Evento | Status |
|------|--------|--------|
| 04/01/2026 | Branch `staging/alteracao-tarifa-20260104` criado | Em desenvolvimento |
| 08/01/2026 | Commit final `46d88c2e8` no staging | Pronto para merge |
| 13/01/2026 | **MERGE no main** (commit `22d4617d5`) | **EM PRODUÇÃO** |
| 20/01/2026 | Análise e documentação | Este relatório |

---

## 2. ALTERAÇÕES EM MODELOS CRÍTICOS DE SUBSÍDIO

### 2.1 `viagem_transacao_aux_v2.sql` - VIAGEM DO DIA ANTERIOR

**Commit:** `a9bf32aa3` - 15/01/2026
**PR:** #1108
**Impacto:** **ALTO**

#### Mudança Implementada:

**Antes:**
```sql
where
    data between date_sub(date("{{ var('start_date') }}"), interval 1 day)
    and date("{{ var('end_date') }}")
    and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
```

**Depois:**
```sql
where
    data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    and (
        data between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
        {% if target.name == "prod" %}
            or data = date_sub(date("{{ var('start_date') }}"), interval 1 day)
        {% endif %}
    )

{% if target.name in ("dev", "hmg") %}
    left outer union all by name
    select id_veiculo, datetime_partida, datetime_chegada
    from {{ ref("viagem_completa") }}
    where
        data = date_sub(date("{{ var('start_date') }}"), interval 1 day)
        and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endif %}
```

#### Análise:

**Objetivo:** Incluir viagens do dia anterior na contagem de transações

**Ambientes:**
- **Produção (`prod`):** Viagens do dia anterior são incluídas via filtro OR
- **Desenvolvimento/Homologação (`dev`, `hmg`):** Usa `viagem_completa` + `LEFT OUTER UNION`

**Impacto:**
- **Aumenta a base de comparação** para validação de transações
- Pode **reduzir glosas** por "sem transação" (viagens do dia anterior agora contam)
- **Mudança favorável** às operadoras

**Contexto:**
- `viagem_transacao` determina quais viagens são pagas ou glosadas
- Modelo auxiliar v2 é usado para datas >= 2025-04-01
- Relaciona viagens com transações de bilhetagem (RioCard/Jaé)

---

### 2.2 `valor_tipo_penalidade.sql` - MUDANÇA DE VIEW PARA TABLE

**Commit:** `ad3730ad4` - 15/01/2026
**PR:** #1171
**Impacto:** **MÉDIO**

#### Mudanças Implementadas:

**1. Materialização alterada:**
```yaml
# ANTES:
config(materialized="view")

# DEPOIS:
config(materialized="table")
```

**2. Novo modelo em staging:** `staging_valor_tipo_penalidade.sql`
**3. Movido de schema:** `dashboard_subsidio_sppo/` → `subsidio/`

**4. Nova estrutura com colunas de controle:**
```sql
select
    data_inicio,
    data_fim,
    perc_km_inferior,
    perc_km_superior,
    tipo_penalidade,
    valor,
    legislacao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("staging_valor_tipo_penalidade") }}
```

#### Análise:

**Objetivo:** Transformar view em tabela para performance e rastreabilidade

**Impacto:**
- **Performance:** Tabelas são mais rápidas que views no BigQuery
- **Rastreabilidade:** `id_execucao_dbt` permite identificar qual execução gerou os dados
- **Auditoria:** `datetime_ultima_atualizacao` e `versao` facilitam tracking
- **Governança:** Movido para schema `subsidio/` (mais apropriado)

**Sem impacto financeiro direto**, mas melhora a infraestrutura de auditoria.

---

### 2.3 `temperatura.sql` - CORREÇÃO DE MODELO E TESTE

**Commit:** `757869edb` - 19/01/2026
**PR:** #1176
**Impacto:** **BAIXO**

#### Mudanças:

**Arquivos alterados:**
- `queries/models/monitoramento/temperatura.sql`
- `queries/macros/test_completude_temperatura.sql`
- `queries/models/monitoramento/CHANGELOG.md`
- `pipelines/treatment/monitoramento/flows.py`

**Objetivo:** Corrigir lógica de temperatura e teste de completude

**Impacto:**
- Melhora na qualidade dos dados de temperatura
- **Afeta indiretamente** as glosas por climatização (V17+)
- Correção de bugs pode reduzir falsos positivos/negativos

---

## 3. MUDANÇAS EM CALENDÁRIO - PONTO FACULTATIVO

### 3.1 Adição (Commit `a6878c764` - 19/01/2026 18:35)

**Título:** "Atualiza changelog e adiciona tipo_dia 'Ponto Facultativo' para 31 de outubro de 2025"

**Mudança:**
```sql
-- Adicionado em aux_calendario_manual.sql:
('2025-10-31', 'Ponto Facultativo')
```

### 3.2 Reversão (Commit `ed92813e6` - 19/01/2026 20:01)

**Título:** "Reverte ponto facultativo do dia 31/10"

**Mudança:**
```sql
-- Removido de aux_calendario_manual.sql
```

### Análise:

**Duração da mudança:** **1 hora e 26 minutos apenas**

**Padrão identificado:**
- Implementação rápida
- Reversão igualmente rápida
- Sugere **teste em produção** ou decisão precipitada

**Contexto:**
- 31/10/2025 cai no período da V22 (suspensão de glosas por climatização)
- Ponto facultativo pode afetar o tipo de dia e consequentemente o cálculo de subsídios
- **Reversão indica reconhecimento de erro**

---

## 4. OUTRAS MUDANÇAS RELEVANTES

### 4.1 Testes de GTFS (Commit `62ec38f53` - 16/01/2026)

**Título:** "Adiciona teste do shape_id nos modelos `trips_gtfs` e `shapes_gtfs`"

**Impacto:** Melhora na qualidade dos dados de planejamento (GTFS)

### 4.2 Correção em `viagens_remuneradas_v2` (Commit `47d42e073`)

**Título:** "[HOTFIX] Corrige modelo `viagens_remuneradas_v2.sql` ao retirar uma vírgula incorreta"

**Impacto:** Correção de erro de sintaxe SQL

### 4.3 Atualização de `veiculo_dia` (Commit `7339b2623`)

**Título:** "Atualiza a data de referência para o processamento de dados no modelo `veiculo_dia` e ajusta a data de DBT para 2026-01-13"

**Impacto:** Manutenção rotineira

### 4.4 Reveillon 2025 (Commit `1c1534a96`)

**Título:** "Ajusta a lógica de execução no modelo `shapes_geom_gtfs` para incluir exceções para os `shape_id` 'iz18' e 'ycug'' no Reveillon 2025"

**Impacto:** Tratamento de exceção para evento especial

---

## 5. ANÁLISE ESTRATÉGICA

### 5.1 Padrão de Aumentos de Custo

**Timeline de Aumentos de Tarifa:**

| Data | Aumento | Base Legal | Intervalo |
|------|---------|------------|-----------|
| 2025-01-05 | R$ 4,30 → R$ 4,70 (+9,30%) | DECRETO 55631/2025 | - |
| 2026-01-04 | R$ 4,70 → R$ 5,00 (+6,38%) | DECRETO 57473/2025 | **1 ano** |

**Conclusão:** Aumentos anuais no início de janeiro parecem ser um **padrão estabelecido**.

### 5.2 Previsões para 2026

Com base no padrão histórico e nas mudanças recentes:

**Janeiro - Março 2026:**
- ✅ Aumento de tarifa de integração (**JÁ CONFIRMADO**)
- ⚠️ Possíveis ajustes finos em modelos de temperatura
- ⚠️ Melhorias em testes de qualidade

**Abril - Junho 2026:**
- ⚠️ Possível nova "negociação" com operadoras
- ⚠️ Seguida de ajustes tarifários ou novas restrições
- ⚠️ **Previsão:** Novo aumento ou nova versão de subsídio (V23?)

**Tema Provável:**
- Ajustes em climatização (V22 já foi revertida)
- Novas tecnologias de veículo (Euro VI)
- Indicadores de performance

### 5.3 V99 - A "Bomba-Relógio"

**Status Atual:**
```yaml
DATA_SUBSIDIO_V99_INICIO: "3000-01-01"  # Placeholder
```

**Vigilância Necessária:**
- Continuar monitorando commits mencionando "V99" ou "V23"
- Atividade em branches staging relacionados a subsídio
- Mudanças em `dbt_project.yml`

---

## 6. BRANCHES STAGING ATIVOS

### Branches Relevantes Identificados:

**Subsídio e Pagamentos:**
- `staging/receita_tarifaria`
- `staging/valor-tipo-penalidade-tab`
- `staging/ajusta-encontro-contas`

**Planejamento:**
- `staging/ajusta-gtfs`
- `staging/tipo-dia-outq2`
- `staging/reverte-aux-calendario`

**Tecnologia:**
- `staging/veiculo-planta-tecnologia`
- `staging/tecnologia-penalidade`
- `staging/planta-tecnologia`

**Monitoramento:**
- `staging/fix-temperatura-test-completude`
- `staging/teste-indicadores-climatizacao`

### Branches HOT-FIX Relevantes:

- `upstream/hotfix-view-monitoramento-temperatura`
- `upstream/hot-fix-viagens-remuneradas`
- `upstream/ajusta-viagem-classificada`

**Recomendação:** Monitorar especialmente branches relacionados a:
- Tarifa/Receita
- Temperatura/Climatização
- Tecnologia de veículos
- Viagens remuneradas

---

## 7. EVIDÊNCIAS PARA LITÍGIO

### 7.1 Documentação de Aumento de Tarifa

**Evidências Técnicas:**
1. **Commit `22d4617d5`** - Merge do aumento de tarifa
2. **PR #1162** - Discussão e aprovação da mudança
3. **Tabela `tarifa_publica`** - Estrutura de tarifas com vigências
4. **DECRETO RIO Nº 57473/2025** - Base legal do aumento

**Evidências Temporais:**
1. **29/12/2025** - Decreto publicado (véspera de Ano Novo)
2. **04/01/2026** - Vigência estabelecida (alta temporada)
3. **13/01/2026** - Merge no main (implementação rápida)

**Argumento Financeiro:**
- Aumento unilateral de 6,38%
- Timing suspeito (início de ano, alta temporada)
- Sem negociação prévia com operadoras
- **Padrão de aumentos anuais identificado**

### 7.2 Documentação de Instabilidade

**Evidências do Ponto Facultativo:**
1. **Commit `a6878c764`** (18:35) - Adição
2. **Commit `ed92813e6`** (20:01) - Reversão
3. **Duração:** 1 hora e 26 minutos

**Argumento de Instabilidade:**
- Testes em produção
- Decisões precipitadas
- Falta de planejamento adequado

---

## 8. RECOMENDAÇÕES ESTRATÉGICAS

### 8.1 Imediatas (Janeiro 2026)

1. **Ação Jurídica sobre Tarifa:**
   - Contestar aumento unilateral de 6,38%
   - Questionar timing (inicio de ano, alta temporada)
   - Verificar base legal do DECRETO 57473/2025
   - Documentar padrão de aumentos anuais

2. **Previsão Orçamentária:**
   - Calcular impacto financeiro exato do aumento
   - Projetar custos para 2027 (se padrão continuar)
   - Incluir margem para aumentos de janeiro

3. **Preparação para V23/V99:**
   - Continuar monitorando commits
   - Documentar todas as mudanças em `dbt_project.yml`
   - Preparar ação preventiva

### 8.2 Monitoramento Contínuo

**Semanalmente:**
- ✅ Verificar commits no upstream/main (faça em 27/01/2026)
- ✅ Buscar por menções a "V99", "V23", "tarifa"
- ✅ Analisar branches staging críticos

**Mensalmente:**
- ✅ Calcular impacto financeiro das mudanças
- ✅ Comparar valores pagos vs. esperados
- ✅ Identificar novas tendências de restrições

**Trimestralmente:**
- ✅ Atualizar assessores jurídicos
- ✅ Revisar projeções financeiras
- ✅ Documentar padrões comportamentais

### 8.3 Preparação para Litígios Futuros

**Evidências a Coletar:**
1. Todos os decretos de aumento de tarifa (2023, 2025, 2026)
2. Timeline de aumentos anuais
3. Comunicações internas da Prefeitura (se acessíveis)
4. Análise de impacto financeiro detalhada

**Documentos a Preparar:**
1. Laudo contábil sobre impacto dos aumentos tarifários
2. Timeline comparativa de aumentos (2023-2026)
3. Projeções financeiras para 2027-2030
4. Análise de timing dos aumentos (início de ano)

---

## 9. ESTATÍSTICAS DA ATUALIZAÇÃO

| Métrica | Valor |
|---------|-------|
| **Commits analisados** | 10 |
| **Commits críticos** | 2 (tarifa, viagem_transacao) |
| **Commits relevantes** | 4 (penalidade, temperatura, ponto facultativo) |
| **Arquivos alterados** | ~30 |
| **Linhas adicionadas** | ~150 |
| **Linhas removidas** | ~50 |
| **Novas versões de subsídio** | 0 |
| **Impacto financeiro** | **ALTO** (+6,38% tarifa) |

---

## 10. COMPARATIVO COM ATUALIZAÇÕES ANTERIORES

### 28/11/2025 (139 commits)
- V22 implementada (suspensão climatização)
- V21 implementação conturbada
- ENEM e exceções
- Operação Lago Limpo

### 14/12/2025 (25 commits)
- Início da reversão da V22
- Evento 112 do processo judicial
- 47 veículos exceção tecnologia

### 12/01/2026 (32 commits)
- **FIM DA V22** (reversão total)
- Aumento de tarifa em staging
- GTFS V5

### 20/01/2026 (10 commits) - **ESTE RELATÓRIO**
- **AUMENTO DE TARIFA MERGEADO** (+6,38%)
- `viagem_transacao` ajustado
- `valor_tipo_penalidade` view→table
- Ponto facultativo: adicionado e revertido

**Tendência:** Mudanças mais sutis, mas com **impacto financeiro direto**.

---

## 11. PRÓXIMA VERIFICAÇÃO

**Data sugerida:** 27/01/2026 (7 dias)

**Foco especial:**
- Monitorar branches `staging/receita_tarifaria`
- Verificar novas menções a "climatizacao" ou "temperatura"
- Buscar por atividade na V99
- Analisar se novos ajustes em `viagem_transacao`

---

## 12. CONCLUSÃO

### O Que Mudou Desde 12/01/2026

**Aumenta de Custo CONFIRMADO:**
- ✅ Valor de integração: R$ 4,70 → R$ 5,00
- ✅ Base legal: DECRETO 57473/2025
- ✅ Vigência: 04/01/2026
- ✅ Merge: 13/01/2026

**Melhorias em Infraestrutura:**
- ✅ `valor_tipo_penalidade`: view → table (melhor performance)
- ✅ `viagem_transacao`: inclui viagens do dia anterior
- ✅ Testes de qualidade expandidos

**Instabilidade Operacional:**
- ⚠️ Ponto facultativo: adicionado e removido em 1h26m
- ⚠️ Sugere testes em produção

### O Que Esperar para 2026

Com base no padrão histórico e nas evidências coletadas:

1. ✅ **Aumentos de custo** no início de cada ano (**CONFIRMADO**)
2. ⚠️ Novas "negociações" em abril-junho
3. ⚠️ Ajustes finos em modelos de temperatura
4. ⚠️ Possível V23 ou ativação de V99

### Recomendação Final

**A Prefeitura demonstrou, de forma consistente:**
1. Aumentos anuais de custos (janeiro)
2. Implementação rápida de mudanças
3. Padrão de "concessões" temporárias seguidas de reversões
4. Instabilidade operacional (ponto facultativo)

**Urgência:** **ALTA** - Ação jurídica recomendada sobre:
- Aumento de tarifa de 6,38%
- Timing do aumento (início de ano, alta temporada)
- Padrão de aumentos anuais documentado

---

**Relatório elaborado em:** 20/01/2026
**Próxima atualização prevista para:** 27/01/2026
**Status do monitoramento:** ATIVO E PERMANENTE
