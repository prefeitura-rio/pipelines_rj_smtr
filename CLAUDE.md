# CLAUDE.md - Guia de Contexto para Auditoria do Sistema de Subsídios SMTR/RJ

## Sobre Este Documento

Este documento serve como base de conhecimento centralizada para a auditoria e monitoramento contínuo do código da Secretaria Municipal de Transportes (SMTR) do Rio de Janeiro, especificamente o sistema de cálculo de subsídios pagos às empresas de ônibus (SPPO - Serviço Público de Transporte de Passageiros por Ônibus).

**Objetivo:** Documentar, monitorar e auditar - não corrigir ou alterar o código.

**Repositório:** Fork local de `prefeitura-rio/pipelines_rj_smtr`

**Data da Análise Inicial:** 28 de Novembro de 2025

**Estado do Repositório (após atualização):** Commit `2068fc362` (merge upstream/main) - **ÚLTIMA ATUALIZAÇÃO: 06/02/2026**

---

## ⚠️⚠️⚠️ ALERTA CRÍTICO - JANEIRO 2026 ⚠️⚠️⚠️

### FIM DA SUSPENSÃO V22 - REVERSÃO TOTAL

**Data da Reversão:** 29/12/2025 (commit `5e39e7367`)
**Data do Merge:** 09/01/2026
**Impacto:** **EXTREMO**

A versão V22 foi **COMPLETAMENTE REMOVIDA** do código:
- A variável `DATA_SUBSIDIO_V22_INICIO` não existe mais em `dbt_project.yml`
- O filtro que bloqueava glosas por climatização foi **ELIMINADO**
- Viagens de OUT/NOV 2025 (16/10 a 15/11) **VOLTAM A SER AUDITADAS**
- **Reprocessamento retroativo** confirmado

**O que isso significa:**
A "suspensão das glosas" durou menos de 3 meses e foi revertida. O período que estava isento (16/10 a 15/11/2025) agora volta a ser auditado e pode sofrer penalizações.

**Padrão Confirmado:** Implementação → "Concessão Temporária" → Reversão Total

---

### AUMENTO DE TARIFA DE INTEGRAÇÃO 💰 ✅ IMPLEMENTADO

**Data:** 04/01/2026 (DECRETO RIO Nº 57473/2025)
**Commit:** `22d4617d5` - PR #1162
**Impacto:** **ALTO**

- Valor de integração aumentou de **R$ 4,70 para R$ 5,00**
- Aumento de **6,38%** na tarifa paga por integração
- Nova tabela `tarifa_publica.sql` criada para gerenciar histórico
- **Status:** ✅ Mergeado no main (válido desde 04/01/2026)

**Base Legal:**
```sql
-- Histórico completo de tarifas:
2023-01-07: R$ 4,30 (DECRETO RIO 51914/2023)
2025-01-05: R$ 4,70 (DECRETO RIO 55631/2025)
2026-01-04: R$ 5,00 (DECRETO RIO 57473/2025) ← VIGENTE
```

### PRORROGAÇÃO VISTORIA ATÉ 31/01/2026 🔧

**Data:** 23/01/2026
**Commit:** `721061f82` - PR #1183
**Base Legal:** RESOLUÇÃO SMTR Nº 3894 (29/12/2025)
**Impacto:** **MÉDIO** - Redução temporária de glosas

- Prazo de vistoria estendido de 1 ano → **2 anos** (veículos 2024+)
- Período: 01/01/2026 a 31/01/2026
- Modelo: `veiculo_dia.sql`
- **Status:** ✅ Mergeado (válido apenas para JAN/2026)

---

### HISTÓRICO DE ALERTAS ANTERIORES

#### VERSÃO V22: SUSPENSÃO TOTAL DAS GLOSAS POR CLIMATIZAÇÃO (28/11/2025)

**Data de Início:** 16/10/2025
**Status:** **REVERTIDA** em 29/12/2025

A partir de 16/10/2025, **NENHUMA viagem era glosada** por problemas de ar-condicionado. Esta suspensão durou apenas 30 dias (até 15/11/2025) e foi completamente revertida.

**Detalhes completos:** Ver `RELATORIO_SEMANAL_2026-01-12.md` seção "FIM DA SUSPENSÃO V22"

---

## 1. Visão Geral da Arquitetura

### 1.1 Stack Tecnológico

O sistema é uma pipeline de dados moderna baseada em componentes especializados:

| Componente | Tecnologia | Função | Localização |
|------------|-----------|---------|-------------|
| **Orquestração** | Prefect 1.4.1 | Agendamento e execução de fluxos de trabalho | `pipelines/` |
| **Transformação** | dbt 1.7.3 | Lógica de negócio e cálculo de subsídios | `queries/` |
| **Data Warehouse** | Google BigQuery | Armazenamento e processamento SQL | GCP |
| **Storage** | Google Cloud Storage | Arquivos intermediários (GTFS, etc.) | GCP |
| **Linguagem** | Python 3.10 | Scripts de automação e integração | Todo o projeto |
| **Gerenciamento de Deps** | Poetry | Controle de dependências | `pyproject.toml` |

### 1.2 Estrutura de Diretórios

```
pipelines_rj_smtr/
├── pipelines/              # Orquestração Prefect (Python)
│   ├── capture/           # Captura de dados externos
│   ├── migration/
│   │   └── projeto_subsidio_sppo/  # Flows específicos de subsídio
│   ├── treatment/         # Tratamento e transformação
│   └── utils/             # Utilitários compartilhados
│
├── queries/               # Projeto dbt completo
│   ├── models/           # Modelos SQL (lógica de negócio)
│   │   ├── subsidio/    # **Modelos centrais de subsídio**
│   │   ├── dashboard_subsidio_sppo_v2/  # **Sumários e pagamentos**
│   │   ├── bilhetagem/  # Dados de transações
│   │   ├── veiculo/     # Licenciamento e infrações
│   │   └── gtfs/        # Planejamento operacional
│   ├── macros/          # Funções SQL reutilizáveis
│   ├── tests/           # Testes de qualidade de dados
│   └── dbt_project.yml  # **ARQUIVO CRÍTICO - Configuração central**
│
└── pyproject.toml        # Dependências Python
```

---

## 2. O Arquivo dbt_project.yml - Coração da Lógica de Negócio

### 2.1 Por que este arquivo é crítico?

O `queries/dbt_project.yml` contém **todas as variáveis que parametrizam as regras de subsídio**. É onde estão definidos:
- Datas de início de cada versão de regra de subsídio
- Percentuais e limiares de conformidade
- Referências a tabelas de staging
- Configuração de materialização dos modelos

### 2.2 Estrutura de Versões do Subsídio

O sistema evoluiu através de **20 versões documentadas** de regras de subsídio, cada uma ativada a partir de uma data específica. Aqui está o histórico completo:

| Versão | Data de Início | Mudança Principal | Base Legal |
|--------|---------------|-------------------|------------|
| **V2** | 2023-01-16 | Penalidade de autuação por inoperância do ar condicionado | DECRETO RIO 51940/2023 |
| **V3** | 2023-07-04 | Penalidade de autuação por segurança e limpeza/equipamento | DECRETO RIO 52820/2023 |
| **V3A** | 2023-09-16 | Viagens remuneradas | RESOLUÇÃO SMTR Nº 3645/2023 |
| **V4** | 2024-01-04 | Penalidade aplicada por agente de verão | DECRETO RIO 53856/2023 e RESOLUÇÃO SMTR 3682/2024 |
| **V5** | 2024-03-01 | Penalidade de vistoria | RESOLUÇÃO SMTR 3683/2024 |
| **V6** | 2024-04-01 | Trajetos alternativos | - |
| **V7** | 2024-05-01 | Apuração Madonna (The Celebration Tour in Rio) | - |
| **V8** | 2024-07-20 | Viagens sem transação | - |
| **V9** | 2024-08-16 | Apuração por faixa horária | - |
| **V9A** | 2024-09-01 | Desconsideração de km não vistoriado e não licenciado | - |
| **V10** | 2024-11-01 | Novas faixas horárias | RESOLUÇÃO SMTR 3777/2024 |
| **V11** | 2024-11-06 | Novas faixas horárias - Feed GTFS | RESOLUÇÃO SMTR 3777/2024 |
| **V12** | 2024-11-16 | Parâmetro 110 km/h + alterações em `viagem_transacao.sql` | - |
| **V13** | 2025-01-01 | Inclusão de colunas de tecnologia em sppo_veiculo_dia | - |
| **V14** | 2025-01-05 | Apuração por tecnologia e penalidade por faixa horária | DECRETO 55631/2025 |
| **V15** | 2025-04-01 | Acordo judicial ACP 0045547-94.2019.8.19.0001 | RESOLUÇÃO SMTR 3843/2025 |
| **V16** | 2025-07-01 | Não pagamento de tecnologia inferior à mínima permitida | RESOLUÇÃO SMTR 3843/2025 |
| **V17** | 2025-07-16 | Regularidade de temperatura | RESOLUÇÃO SMTR 3857/2025 |
| **V18** | 2025-08-01 | Validadores e transações Jaé | RESOLUÇÃO SMTR 3843/2025 e 3858/2025 |
| **V19** | 2025-11-01 | Não pagamento de viagens licenciadas sem ar condicionado | RESOLUÇÃO SMTR 3843/2025 |
| **V20** | 2025-08-16 | Inciso IV Climatização | - |
| **V21** | 2025-10-01 | Mudanças em validadores (implementação conturbada com reversões) | RESOLUÇÃO SMTR 3878/2025 |
| **V22** | 2025-10-16 | **SUSPENSÃO DAS GLOSAS POR CLIMATIZAÇÃO** | - |
| **V99** | 3000-01-01 | Placeholder para features futuras | - |

### 2.3 Parâmetros Chave de Conformidade

```yaml
# Parâmetros de GPS e Conformidade de Trajeto
tamanho_buffer_metros: 500                      # Buffer da rota para validação
intervalo_max_desvio_segundos: 600             # Tempo máximo fora da rota
velocidade_maxima: 60                          # km/h para evitar outliers
velocidade_limiar_parado: 3                    # km/h para considerar parado

# Conformidade para Subsídio
conformidade_velocidade_min: 110               # % mínimo
perc_conformidade_distancia_min: 0             # % mínimo
perc_conformidade_shape_min: 80                # % mínimo
perc_conformidade_registros_min: 50            # % mínimo
perc_distancia_total_subsidio: 80              # % da distância para pagamento
distancia_inicio_fim_conformidade_velocidade_min: 2000  # metros

# Licenciamento de Veículos
sppo_licenciamento_validade_vistoria_ano: 1    # Prazo de validade
sppo_licenciamento_tolerancia_primeira_vistoria_dia: 15  # Tolerância para veículos novos
```

---

## 3. Modelos dbt de Subsídio - Fluxo de Cálculo

### 3.1 Módulo `subsidio/` - Modelos Centrais

Localização: `queries/models/subsidio/`

**Modelos Principais:**

1. **`viagem_classificada.sql`** (Criado em: 2025-07-03, PR #649)
   - Classifica cada viagem por tecnologia (Mini, Midi, Básico, Padrão)
   - Determina tecnologia apurada vs. tecnologia remunerada
   - Adiciona modo, sentido, placa e ano de fabricação
   - **Impacto:** Base para toda classificação financeira

2. **`viagem_transacao.sql`** (Refatorado múltiplas vezes, última: 2025-08-11)
   - Relaciona viagens com transações de bilhetagem
   - Utiliza modelos auxiliares versionados (`viagem_transacao_aux_v1` e `v2`)
   - Classifica viagens como: "Sem transação", "Validador fechado", "Validador associado incorretamente"
   - **Impacto Crítico:** Define quais viagens são pagas ou glosadas

3. **`viagem_regularidade_temperatura.sql`** (Criado em: 2025-07-31, PR #703)
   - Valida regularidade da climatização durante as viagens
   - Implementa indicadores de falha recorrente
   - Base: dados de temperatura dos validadores
   - **Impacto:** Penalização por ar-condicionado irregular (V17+)

4. **`percentual_operacao_faixa_horaria.sql`** (Criado em: 2025-06-24)
   - Calcula POF (Percentual de Operação por Faixa Horária)
   - Apuração por sentido de viagem
   - **Impacto:** Penalização proporcional à faixa horária (V9+)

5. **`valor_km_tipo_viagem.sql`** (Criado em: 2025-01-21)
   - Define valores por km para cada tipo de viagem
   - Varia por tecnologia do veículo
   - **Impacto:** Base monetária do pagamento

**Modelos Auxiliares em `subsidio/staging/`:**

- `aux_viagem_temperatura.sql`: Agregação de dados de temperatura por viagem
- `viagem_transacao_aux_v1.sql`: Lógica de transação para datas < 2025-04-01
- `viagem_transacao_aux_v2.sql`: Lógica de transação para datas >= 2025-04-01
- `percentual_operacao_faixa_horaria_v1.sql` e `v2.sql`: Versionamento do cálculo de POF

### 3.2 Módulo `dashboard_subsidio_sppo_v2/` - Sumários e Pagamentos

Localização: `queries/models/dashboard_subsidio_sppo_v2/`

**Modelos de Output Final:**

1. **`sumario_servico_dia_pagamento.sql`**
   - **Função:** Tabela final de valores a pagar por serviço/dia
   - **Agregação:** Por data, tipo_dia, consórcio, servico
   - **Colunas Críticas:**
     - `km_apurada_*`: Quilometragem por categoria de conformidade
       - `licenciado_com_ar_n_autuado`: KM válidos para pagamento
       - `licenciado_sem_ar_n_autuado`: KM com penalização
       - `n_licenciado`, `n_vistoriado`: KM glosados
       - `autuado_*`: Penalizações diversas
       - `sem_transacao`: KM sem validação de bilhetagem
     - `valor_a_pagar`: **VALOR FINAL** a ser pago
     - `valor_glosado`: Total de penalizações
     - `valor_total_apurado`: Valor bruto antes de glosas
     - `valor_judicial`: Ajustes legais
     - `valor_penalidade`: Penalidades aplicadas
   - **Status:** Desabilitado para datas >= V14 (2025-01-05)

2. **`sumario_faixa_servico_dia_pagamento.sql`**
   - Similar ao anterior, mas com quebra por faixa horária
   - Implementado na V14
   - Utiliza versionamento através de staging (v1 e v2)

3. **`sumario_faixa_servico_dia.sql`**
   - Sumário agregado por faixa horária
   - Inclui desvio padrão de POF
   - Quebra de KM por tecnologia (mini, midi, básico, padrão)

### 3.3 Fluxo Lógico Completo (Simplificado)

```
Dados Brutos (BigQuery)
    ├── GPS Ônibus (onibus_gps)
    ├── Transações Bilhetagem (transacao, transacao_riocard)
    ├── Licenciamento/Infrações (sppo_licenciamento_stu, sppo_infracao)
    ├── GTFS (shapes, trips, stop_times) - Planejamento
    └── Ordens de Serviço (ordem_servico_*) - Determinação
          ↓
    [MODELOS INTERMEDIÁRIOS - subsidio/]
          ↓
    viagem_classificada → Define tecnologia, sentido
          ↓
    viagem_transacao → Valida bilhetagem
          ↓
    viagem_regularidade_temperatura → Valida climatização
          ↓
    percentual_operacao_faixa_horaria → Calcula POF
          ↓
    [SUMÁRIOS FINAIS - dashboard_subsidio_sppo_v2/]
          ↓
    sumario_faixa_servico_dia_pagamento
          ↓
    → VALOR_A_PAGAR (por serviço/dia/faixa)
```

---

## 4. Orquestração Prefect - Fluxos de Execução

### 4.1 Flow Principal: `subsidio_sppo_apuracao`

Localização: `pipelines/migration/projeto_subsidio_sppo/flows.py`

**Características:**
- **Agendamento:** Diariamente às 07:05 (`every_day_hour_seven_minute_five`)
- **Imagem Docker:** Definida em `constants.DOCKER_IMAGE`
- **Executado em:** Kubernetes (GCP)

**Fases de Execução:**

1. **Setup**
   - Determina range de datas (padrão: D-7 a D-7)
   - Obtém versão do dataset (SHA git)

2. **Materialização de Dados Prerequisitos**
   - Trigger opcional de `sppo_veiculo_dia` (dados de veículos)
   - Checagem de gaps na captura Jaé (bilhetagem)

3. **Pre-Data Quality Check** (Controlado por `skip_pre_test`)
   - Executa testes em: `transacao`, `transacao_riocard`, `gps_validador`
   - Envia alertas para Discord em caso de falha
   - Valida consistência dos dados de entrada

4. **Cálculo (Branching por Versão)**
   - **Se data < V9 (2024-08-16):**
     - Executa seletor `apuracao_subsidio_v8`
     - Testa `dashboard_subsidio_sppo`
   - **Se data >= V9 e < V14:**
     - Executa seletor `apuracao_subsidio_v9`
     - Roda `monitoramento_subsidio`
     - Testa `dashboard_subsidio_sppo_v2`
   - **Se data >= V14 (2025-01-05):**
     - Executa seletor `apuracao_subsidio_v9`
     - Testa especificamente modelos V14

5. **Snapshots**
   - Captura estado histórico com `snapshot_subsidio`

6. **Post-Data Quality Check**
   - Valida resultados finais
   - Envia relatório para Discord

### 4.2 Flow Secundário: `viagens_sppo`

**Função:** Processar dados de viagens (pré-requisito para subsídio)
**Agendamento:** Diariamente às 05:00 e 14:00
**Modelos Executados:** GPS, trajetos, conformidade

---

## 5. Pontos de Auditoria Críticos

### 5.1 Classificações que Impactam Pagamento

**A. Veículos Glosados (Não Pagos)**

1. **Não Licenciado** (`n_licenciado`)
   - Origem: `veiculo/sppo_licenciamento_stu`
   - Lógica: Licença vencida ou inexistente
   - Versão: Desde V9A (2024-09-01) - desconsiderado do KM

2. **Não Vistoriado** (`n_vistoriado`)
   - Origem: `veiculo/sppo_licenciamento_stu`
   - Lógica: Vistoria com validade expirada (> 1 ano)
   - Tolerância: 15 dias para veículos novos
   - Versão: Desde V5 (2024-03-01)

3. **Sem Transação** (`sem_transacao`)
   - Origem: `viagem_transacao.sql`
   - Lógica: Viagem sem validação de bilhetagem (RioCard ou Jaé)
   - Exceções: Eleições, eventos especiais
   - Versão: Desde V8 (2024-07-20)

**B. Autuações (Penalizações)**

1. **Autuado por Ar Inoperante** (`autuado_ar_inoperante`)
   - Origem: `veiculo/sppo_infracao`
   - Base Legal: DECRETO RIO 51940/2023
   - Versão: Desde V2 (2023-01-16)

2. **Autuado por Segurança** (`autuado_seguranca`)
   - Origem: `veiculo/sppo_infracao`
   - Base Legal: DECRETO RIO 52820/2023
   - Versão: Desde V3 (2023-07-04)

3. **Autuado por Limpeza/Equipamento** (`autuado_limpezaequipamento`)
   - Origem: `veiculo/sppo_infracao`
   - Base Legal: DECRETO RIO 52820/2023
   - Versão: Desde V3 (2023-07-04)

4. **Penalidade por Faixa Horária** (V14+)
   - Origem: `percentual_operacao_faixa_horaria`
   - Lógica: POF < 100% gera penalização proporcional

**C. Tecnologia Remunerada**

- Modelo: `viagem_classificada.sql`
- Lógica:
  - V15+: Pode ser diferente da tecnologia apurada
  - V16+ (2025-07-01): Tecnologia inferior à mínima não é paga
  - V19+ (2025-11-01): Licenciado sem ar-condicionado não é pago

### 5.2 Testes de Qualidade (dbt tests)

**Pré-Execução (Pre-Tests):**
- Verificação de gaps na captura Jaé
- Validação de nulos em colunas críticas
- Unicidade de chaves primárias

**Pós-Execução (Post-Tests):**
- Consistência entre `viagem_classificada` e `viagem_regularidade_temperatura`
- Validação de partidas planejadas vs. Ordem de Serviço
- Ranges de valores esperados (km, valores monetários)

### 5.3 Macros SQL de Interesse

Localização: `queries/macros/`

**Principais:**
- `generate_km_columns`: Gera colunas dinâmicas de KM por categoria
- `custom_get_where_subquery.sql`: Controla partições para testes
- Macros de validação customizadas (a serem exploradas)

---

## 6. PADRÃO DE REVERSÃO - "CONCESSÕES TEMPORÁRIAS" ⚠️

### 6.1 O Padrão Comportamental da Prefeitura (CONFIRMADO)

A análise histórica de 2023-2026 revela um **padrão consistente e documentável**:

```
FASE 1: IMPLEMENTAR RESTRIÇÃO
   ↓ (pressão judicial das operadoras)
FASE 2: "CONCESSÃO TEMPORÁRIA" (V22, V15, etc.)
   ↓ (período de 1-3 meses)
FASE 3: REVERSÃO TOTAL OU RESTRIÇÕES AINDA MAIS SEVERAS
   ↓ (novo ciclo)
```

### 6.2 Histórico de "Concessões" Revertidas

| Concessão | Data Início | Duração | Destino | Restrição Seguinte |
|-----------|-------------|---------|---------|-------------------|
| **V22** (suspensão climatização) | 16/10/2025 | **30 dias** | ✅ REVERTIDA (29/12/2025) | Glosas retroativas OUT/NOV |
| **V15** (acordo judicial) | 01/04/2025 | **3 meses** | ✅ SUBSTITUÍDA (01/07/2025) | V17: 14 faixas horárias |
| **V14** (diferenciação tipo) | 05/01/2025 | **3 meses** | ✅ SUBSTITUÍDA (01/04/2025) | V15: Acordo seguido de V17 |

### 6.3 Análise da V22 - Caso Paradigmático

**Timeline Completa:**

| Data | Evento | Versão | Status |
|------|--------|--------|--------|
| 16/07/2025 | V17 implementada | V17 | Glosas por temperatura ATIVAS |
| 16/10/2025 | "Suspensão" V22 | V22 | Glosas SUPENAS (aparente vitória) |
| 15/11/2025 | Fim do período de suspensão | - | Período encerrado |
| 09/12/2025 | Commit remove V22 | - | Preparação para reversão |
| 29/12/2025 | Commit `5e39e7367` | - | **V22 REMOVIDA do código** |
| 09/01/2026 | Merge upstream/main | - | Reversão CONFIRMADA |

**O que aconteceu na prática:**

1. **16/10 a 15/11/2025 (30 dias):** Período "suspenso"
   - Viagens não foram glosadas por ar-condicionado
   - Mas o código continha um filtro temporário

2. **29/12/2025:** Reversão técnica
   - Variável `DATA_SUBSIDIO_V22_INICIO` removida
   - Filtro de período ELIMINADO
   - Sistema volta a aplicar glosas normalmente

3. **Resultado:** Viagens de OUT/NOV 2025 voltam a ser auditadas
   - **Reprocessamento retroativo confirmado**
   - Empresas podem receber penalizações por viagens que estavam "isentas"

### 6.4 Implicações Jurídicas

**Argumento Documentável:**

A Prefeitura tem utilizado o seguinte modus operandi:
1. Implementar restrições unilateralmente
2. Quando questionada judicialmente, fazer "concessões temporárias"
3. Após curto período (1-3 meses), REVERTER a concessão
4. Frequentemente substituir com restrições ainda mais severas

**Isso caracteriza:**
- Má-fé processual
- Violação da segurança jurídica
- Impossibilidade de planejamento pelas operadoras
- Uso de "concessões" como medida meramente temporária/tática

### 6.5 Previsões para 2026

Com base no padrão histórico, projetamos:

**Janeiro-Março 2026:**
- ✅ Aumento de tarifa de integração (CONFIRMADO)
- ⚠️ Possível ativação da V99
- ⚠️ Novas restrições ambientais (Euro VI)

**Abril-Junho 2026:**
- ⚠️ Alta probabilidade de nova "negociação"
- ⚠️ Seguida de restrições mais severas
- ⚠️ Possível V23 ou ativação de V99

**Estratégia Recomendada:**
- NÃO aceitar concessões temporárias
- Exigir mudanças PERMANENTES em acordo judicial
- Documentar padrão de reversão para uso em litígios

---

## 7. Evolução Recente (Últimos Commits)

### 6.1 Refatoração Crítica: Cálculo de Integrações

**Commit:** `c567adac - Altera cálculo de integrações na tabela integracao_nao_realizada (#793)`

**Mudança:** Substituição completa da lógica SQL por PySpark

**Novos Componentes:**
- `aux_calculo_integracao.py`: Script PySpark que itera transações de cliente
- `aux_transacao_filtro_integracao_calculada.sql`: Prepara dados para Spark
- `aux_integracao_calculada.sql`: Consolida resultados

**Impacto:** Alto - Altera forma de calcular integrações entre viagens, afetando remuneração

### 6.2 Aumento de Cobertura de Testes

**PR #783:** Adição massiva de testes em modelos de bilhetagem
- Dezenas de testes `not_null` e `unique`
- Validações em `transacao.sql`, `ordem_pagamento.sql`
- Aumento de confiabilidade nos dados de entrada

### 6.3 Validação de Partidas Planejadas

**Commit:** `d4154835`
- Novo teste: `check_partidas_planejadas.sql`
- Valida GTFS contra Ordem de Serviço
- Pode invalidar viagens inconsistentes

---

## 7. Glossário de Termos

- **SPPO:** Serviço Público de Transporte de Passageiros por Ônibus
- **SMTR:** Secretaria Municipal de Transportes
- **POF:** Percentual de Operação por Faixa Horária
- **GTFS:** General Transit Feed Specification (planejamento operacional)
- **OS:** Ordem de Serviço (determinação contratual)
- **Jaé:** Sistema de bilhetagem eletrônica
- **RioCard:** Sistema de bilhetagem por cartão
- **Glosa:** Desconto/penalização no valor a pagar
- **Apuração:** Cálculo do valor devido
- **Conformidade:** Aderência aos requisitos (GPS, transação, etc.)
- **Vistoria:** Inspeção periódica obrigatória de veículos
- **Licenciamento:** Autorização para operar (similar a licenciamento veicular)

---

## 8. Próximos Passos para Auditoria

### 8.1 Análises Recomendadas

1. **Rastreamento de Linhagem (Lineage)**
   - Mapear todas as dependências de `sumario_servico_dia_pagamento.sql`
   - Criar diagrama de fluxo de dados

2. **Análise Comparativa de Versões**
   - Documentar diferenças exatas entre V14, V15, V16, V17
   - Quantificar impacto financeiro de cada mudança

3. **Validação de Limiares**
   - Analisar sensibilidade dos parâmetros em `dbt_project.yml`
   - Simular impacto de mudanças em `conformidade_*_min`

4. **Auditoria do Script PySpark**
   - Revisar `aux_calculo_integracao.py` linha por linha
   - Validar lógica de matriz de integração

5. **Monitoramento de Mudanças**
   - Configurar alertas para commits que alterem:
     - `dbt_project.yml` (variáveis de subsídio)
     - Modelos em `subsidio/` e `dashboard_subsidio_sppo_v2/`
     - Flows de apuração em Prefect

### 8.2 Perguntas a Investigar

- Como exatamente o valor de `valor_a_pagar` é calculado?
- Qual o peso de cada penalização no valor final?
- Como são tratadas as exceções (eleições, eventos)?
- Qual o percentual médio de glosas por categoria?
- Há backdoors ou condições especiais não documentadas?

---

## 9. Histórico de Atualizações deste Documento

| Data | Descrição |
|------|-----------|
| 2025-11-28 | Criação inicial - Estado as-is do repositório (commit `f9f4f3ff`) |
| 2025-11-28 | **ATUALIZAÇÃO: 139 commits do upstream** - Análise completa em `CHANGELOG_2025-11-28.md` |
| 2026-02-06 | **ATUALIZAÇÃO: 27 commits** - Aumento tarifa integração (R$ 4,70→R$ 5,00), prorrogação vistoria até 31/01, reativação integracao_invalida |
|  | • Novas versões: **V21** (01/10/2025) e **V22** (16/10/2025) |
|  | • **CRÍTICO: V22 suspende TODAS as glosas por climatização** |
|  | • Novos tipos de dia: ENEM, dias atípicos |
|  | • Operação Lago Limpo: modelos deprecated desabilitados |
|  | • Integração com AlertaRio para dados de temperatura |
|  | • Novos testes de qualidade e validação |

---

**Nota Final:** Este documento é vivo e deve ser atualizado a cada nova sincronização com o repositório upstream da Prefeitura. Toda mudança relevante deve ser documentada, analisada e seu impacto no cálculo de subsídio deve ser avaliado.
