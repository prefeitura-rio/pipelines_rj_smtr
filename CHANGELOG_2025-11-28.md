# Changelog - Atualização Upstream (28/11/2025)

## Sumário Executivo

**Total de Commits:** 139 (95 commits relevantes + 44 merges)
**Período:** Após commit `f9f4f3ff` até `c7057208`
**Data da Atualização:** 28 de Novembro de 2025

---

## 🚨 MUDANÇAS CRÍTICAS - IMPACTO DIRETO NO SUBSÍDIO

### 1. SUSPENSÃO DAS GLOSAS POR CLIMATIZAÇÃO (V22)

**Commit:** `ff26edc2` - Altera data do modelo `viagem_regularidade_temperatura`
**Data de Início:** 16/10/2025 (`DATA_SUBSIDIO_V22_INICIO`)
**Impacto:** **ALTÍSSIMO** - Suspensão temporária de todas as glosas por ar-condicionado

**Mudança no Código:**
```sql
# ANTES:
(vt.ano_fabricacao <= 2019 or vt.data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}'))
and not vt.indicador_temperatura_nula_viagem

# DEPOIS (V22):
(vt.ano_fabricacao <= 2019 or vt.data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}'))
and not vt.indicador_temperatura_nula_viagem
and (vt.data < date('{{ var("DATA_SUBSIDIO_V22_INICIO") }}'))  # <-- NOVA CONDIÇÃO
```

**Consequência:** A partir de 16/10/2025, **NENHUMA viagem é glosada por problemas de climatização**, independentemente da temperatura registrada. Isso representa uma reversão temporária das regras introduzidas nas versões V17, V18, V19 e V20.

**Arquivos Afetados:**
- `queries/models/subsidio/viagem_regularidade_temperatura.sql`
- `queries/dbt_project.yml`

---

### 2. NOVA VERSÃO V21 - RESOLUÇÃO SMTR 3878/2025

**Data de Início:** 01/10/2025 (`DATA_SUBSIDIO_V21_INICIO`)
**Base Legal:** RESOLUÇÃO SMTR 3878/2025
**Impacto:** Médio

**Mudanças Identificadas:**
- Alteração nas regras de validador (commits `e44e8567`, `23bd1eeb`)
- Data de início das glosas `Validador fechado` e `Validador associado incorretamente` foi **alterada E REVERTIDA**
  - Primeira tentativa: Alterar para 01/10/2025 (commit `e44e8567`)
  - Reversão: Voltou ao padrão anterior (commit `23bd1eeb`)
- Exceção criada para 10/10/2025 (email `2025-10-10T15:08`)

**Commits Relevantes:**
- `e44e8567`: Alterada data para 2025-10-01
- `23bd1eeb`: Revertida alteração

**Análise:** Esta versão teve implementação conturbada, com mudanças sendo revertidas. Sugere incerteza regulatória ou problemas de implementação.

---

### 3. EXCEÇÕES PARA EXAME ENEM

**Commits:** `c7057208`, `80290414`, `4e95af66`, `d39028be`, `778b6fa7`
**Tipo de Dia:** `ENEM` e `V+ENEM`
**Impacto:** Médio

**Mudanças:**
- Criação de novo tipo de dia especial para o ENEM (similar a eleições, carnaval)
- Ajustes em `ordem_servico_trips_shapes_gtfs_v2.sql` para tratar esses dias
- Múltiplos hotfixes indicam complexidade na implementação

**Arquivos Afetados:**
- `queries/models/planejamento/ordem_servico_trips_shapes_gtfs_v2.sql`
- `queries/models/datario/calendario.sql`

---

### 4. OPERAÇÃO LAGO LIMPO - DESABILITAÇÃO DE MODELOS

**Commit:** `1ca1a0a9`, `dcc184e2`
**Impacto:** Organizacional

**Ação:** Modelos antigos de subsídio foram marcados como `deprecated` e **desabilitados**:
- `dashboard_subsidio_sppo/*` (exceto v2)
- `projeto_multa_automatica/*`
- `operacao/*`
- `br_rj_riodejaneiro_geo/deprecated/*`

**Configuração em dbt_project.yml:**
```yaml
dashboard_subsidio_sppo:
  +materialized: view
  +schema: dashboard_subsidio_sppo
  deprecated:
    +materialized: view
    +schema: dashboard_subsidio_sppo
    +enabled: false  # <-- DESABILITADO
```

**Consequência:** Limpeza de código legado. Apenas modelos V2+ estão ativos.

---

## 📊 MUDANÇAS EM PARÂMETROS E CONFIGURAÇÕES

### dbt_project.yml

| Variável | Mudança | Valor Anterior | Valor Novo |
|----------|---------|----------------|------------|
| `DATA_SUBSIDIO_V21_INICIO` | **ADICIONADA** | N/A | `2025-10-01` |
| `DATA_SUBSIDIO_V22_INICIO` | **ADICIONADA** | N/A | `2025-10-16` |
| `DATA_SUBSIDIO_V99_INICIO` | Comentário alterado | "Associação de serviços corretamente" | "Placeholder feature" |
| `data_processamento_veiculo_licenciamento_dia` | Elemento adicionado | `["'2025-07-10'", "'2025-07-24'"]` | `["'2025-07-10'", "'2025-07-24'", "'2025-10-16'"]` |

### Novos Schemas Configurados

- `br_rj_riodejaneiro_geo` (com deprecated)
- `br_rj_riodejaneiro_transporte`
- `dashboard_subsidio_van` (novo!)
- `dashboard_gps_sppo`
- `dashboards`
- `operacao` (com deprecated)
- `projeto_multa_automatica` (com deprecated)
- `brt_manutencao` (deprecated)
- `br_rj_riodejaneiro_stpl_gps` (deprecated)

---

## 🔧 MUDANÇAS NOS MODELOS SQL DE SUBSÍDIO

### Modelos Modificados (M)

| Arquivo | PRs Principais | Mudança Chave |
|---------|---------------|---------------|
| `viagem_regularidade_temperatura.sql` | #1069, #993, #1002, #996 | **Suspensão de glosas V22** + ajustes de indicadores |
| `viagem_classificada.sql` | #990 | Correção de variável V17→V16, novo teste `test_check_tecnologia_minima` |
| `aux_viagem_temperatura.sql` | #1068, #996, #985, #977, #1000 | Filtro de GPS fora de garagens, correção de nulos, integração com AlertaRio |
| `viagem_transacao_aux_v1.sql` | #985 | Correção de `indicador_estado_equipamento_aberto` quando `id_validador` é nulo |
| `servico_contrato_abreviado.sql` | #1058 | Alteração de referências |
| `valor_km_tipo_viagem.sql` | #1058 | Alteração de referências |
| `viagens_remuneradas_v1.sql` | - | Ajustes não documentados |
| `viagens_remuneradas_v2.sql` | - | Ajustes não documentados |

### Modelos Adicionados (A)

| Arquivo | Objetivo |
|---------|----------|
| `dicionario_subsidio.sql` | Documentação/metadados (#926) |
| `staging_servico_contrato_abreviado.sql` | Versionamento de lógica |
| `staging_valor_km_tipo_viagem.sql` | Versionamento de lógica |
| `dicionario_dashboard_subsidio_sppo.sql` | Documentação |
| `subsidio_ordem_servico.sql` | Nova agregação |
| `viagem_climatizacao.sql` | **NOVO MODELO** para análise de climatização |
| `sumario_servico_glosa_dia.sql` (deprecated) | Modelo legado movido |
| `sumario_servico_dia_pagamento_historico.sql` (deprecated) | Histórico |

---

## 🧪 NOVOS TESTES E VALIDAÇÕES

### Testes de Qualidade Adicionados

1. **`test_check_tecnologia_minima`** (#990)
   - Valida que tecnologia remunerada >= tecnologia mínima permitida
   - Aplicado em `viagem_classificada.sql`

2. **`test_check_consistencia`** (#928)
   - Teste de consistência entre tabelas (detalhes a serem explorados)

3. **Teste de Vistoria com Troca de Placa** (#1048)
   - Validação: `data_ultima_vistoria` e `ano_ultima_vistoria`
   - Mudança de regra: Ao trocar placa, vistoria é reavaliada

4. **Testes de Bilhetagem** (múltiplos PRs)
   - Sincronia entre BigQuery e Postgres CCT (#974, #999)
   - Verificação de captura de gratuidades (#1037, #1066)
   - Verificação de captura de `cliente` (#1029)
   - Verificação de captura de `transacao_gratuidade_estudante_municipal` (#1011)

### Validações de Planejamento

- **Teste de Serviço na Tabela de Tecnologia** (#1033)
  - Verifica se todo serviço planejado possui tecnologia definida

---

## 💾 MUDANÇAS EM BILHETAGEM

### Sistema Postgres da CCT

**Nova Infraestrutura:** Transações do BigQuery agora são sincronizadas para Postgres (#952)

**Commits Relevantes:**
- `b314aec1`: Sobe transações do BigQuery para Postgresql da CCT
- `49c470f3`: Altera IP do banco `transacao_db` da Jaé
- `62cc1b56`: Atualiza host do banco de dados principal para novo IP (#1071)

**Impacto:** Duplicação de dados para análises pela CCT (Centro de Controle de Transportes)

### Novos Modelos de Bilhetagem

- `alerta_transacao.sql` (#944) - Posteriormente desativado (#956)
- `gratuidade_estudante_view` (#958) - Modelo incremental

### Correções de Captura

- Ajustes em `integracao` (#959)
- Correções de `id_lancamento` nulo (#948)
- Ajustes em gratuidades (#1035, #1066)

---

## 🌡️ DADOS DE TEMPERATURA - INTEGRAÇÃO COM ALERTARIO

**Commits:** `2481e434`, `762dfa6e`, `12cfbacd`, `596c173c`

**Mudança:** Criação de `temperatura_alertario.sql`
- Fonte: Sistema AlertaRio (meteorologia)
- Uso: Complementar dados de temperatura do INMET
- Integração: Modelo `aux_viagem_temperatura` agora referencia `temperatura` (unificado)

**CTE renomeada:**
```sql
# ANTES:
temperatura_inmet

# DEPOIS:
temperatura_inmet_alertario
```

---

## 🚗 VEÍCULOS E LICENCIAMENTO

### Mudança de Regra de Vistoria (#1048)

**Nova Regra:** Ao trocar a placa de um veículo, a vistoria é reavaliada
- Testes criados para `data_ultima_vistoria` e `ano_ultima_vistoria`

### Processamento de Licenciamento

**Nova data adicionada:** `2025-10-16` em `data_processamento_veiculo_licenciamento_dia`

### Correção de Duplicidade de Autuações (#967)

**Problema:** Autuações estavam sendo contadas em duplicidade
**Solução:** Ajustes no modelo de autuações (detalhes em PR #967)

---

## 📍 GPS E TRAJETOS

### Filtro de GPS em Garagens (#996, #1007, #1032)

**Mudança Crítica:** Pontos de GPS dentro de garagens ou endereços de manutenção são **excluídos** do cálculo de:
- `indicador_gps_servico_divergente`
- `indicador_estado_equipamento_aberto`

**Modelo:** `aux_viagem_temperatura.sql` e `aux_gps_parada.sql`

**Razão:** Evitar falsos positivos quando validadores estão em manutenção

### Correção de Sentido (#989)

**Modelo:** `ordem_servico_faixa_horaria_sentido.sql`
**Problema:** Coluna `sentido` estava incorreta

### Trajetos Alternativos (#1040)

**Modelo:** `ordem_servico_trips_shapes_gtfs_v2.sql`
**Correção:** Distância planejada dos trajetos alternativos estava errada

---

## 📅 CALENDÁRIO E TIPO DE DIA

### Novos Tipos de Dia

| Tipo de Dia | Data(s) | Commit |
|-------------|---------|--------|
| `ENEM` | Datas do ENEM 2025 | `c7057208`, `80290414` |
| `V+ENEM` | Variação com véspera | `80290414` |
| Dia do Comerciário | Data específica | `778b6fa7` |
| Dias Atípicos | Múltiplas datas | `d39028be` |

### Exceções Adicionadas

**Tabela:** `encontro_contas_datas_excecoes` (presumido)
- ENEM 2025
- Operação Lago Limpo (?)

---

## 🏗️ INFRAESTRUTURA E FLOWS

### Mudanças em IPs de Banco de Dados

- **Banco Principal:** Novo IP (#1071)
- **Banco Jaé (transacao_db):** Novo IP (#972)

### Novos Flows Registrados

- Flows de monitoramento (#953, #1013)
- Testes do AlertaRio e temperatura (#1013)
- Flow de captura STU (#919)

### Schedule Alterado

- `VALIDACAO_DADOS_JAE_MATERIALIZACAO`: Schedule modificado (#1039)

### Otimizações

- Query Postgres otimizada para verificação de captura (#1060, #1061, #1062)
- Paralelismo em tasks para execuções locais (#972)

---

## 📁 ORGANIZAÇÃO E GOVERNANÇA

### Mutirão de Governança (#1016)

**Ações:**
- Criação de pasta `deprecated/` em múltiplos schemas
- Alteração de source da tabela `aux_preco_bigquery`

### Criação de Pasta `geo/` (#1015)

**Localização:** `queries/models/br_rj_riodejaneiro_geo/`
**Conteúdo:** Modelos geoespaciais (garagens, limites, etc.)

### Modelos Movidos para `monitoramento_interno/` (#1024)

**Antes:** `monitoramento/`
**Depois:** `monitoramento_interno/`

**Modelos afetados:**
- `monitoramento_sumario_servico_dia_tipo_viagem_historico`
- `monitoramento_servico_dia_tipo_viagem`

---

## 🔍 ANÁLISE DE PADRÕES E TENDÊNCIAS

### 1. **Hiperatividade Regulatória**

A introdução de V21 e V22 em um intervalo de apenas **15 dias** (01/10 → 16/10) demonstra:
- Pressão política ou judicial para suspender glosas de climatização
- Implementação apressada (múltiplos hotfixes)
- Possível litígio ou questionamento das regras V17-V20

### 2. **Reversões e Incertezas**

Commits que foram **revertidos**:
- Data de glosa de validador (V21) - alterada e revertida
- Modelo `alerta_transacao` - criado e desativado

**Interpretação:** Instabilidade nas decisões ou testes em produção.

### 3. **Complexidade Crescente**

**Novos tipos de exceções:**
- Dias especiais (ENEM)
- Exceções pontuais (10/10/2025)
- Filtros geoespaciais (garagens)

**Resultado:** Código cada vez mais difícil de auditar.

### 4. **Integração com Sistemas Externos**

- AlertaRio (temperatura)
- Postgres CCT (bilhetagem)
- STU (captura de dados)

**Tendência:** Dependência de múltiplas fontes aumenta pontos de falha.

---

## ⚠️ PONTOS DE ATENÇÃO PARA AUDITORIA

### 1. **Suspensão de Glosa de Climatização (V22)**

**Por que foi suspensa?**
- Litígio judicial?
- Problemas técnicos nos sensores?
- Pressão das operadoras?

**Até quando?**
- `DATA_SUBSIDIO_V22_INICIO` está em `2025-10-16`, mas não há `DATA_SUBSIDIO_V23_INICIO` para reativação
- Indefinida?

### 2. **Mudanças Não Documentadas**

Modelos alterados sem CHANGELOG claro:
- `viagens_remuneradas_v1.sql`
- `viagens_remuneradas_v2.sql`

### 3. **Novos Modelos Sem Documentação**

- `viagem_climatizacao.sql` - Qual sua função exata?
- `subsidio_ordem_servico.sql` - Como se relaciona com pagamento?

### 4. **Exceções Ad-Hoc**

Exceção para 10/10/2025 referenciada apenas por email (`2025-10-10T15:08`).
**Problema:** Falta de rastreabilidade formal.

---

## 📈 IMPACTO FINANCEIRO ESTIMADO

### V22 - Suspensão de Glosa de Climatização

**Período:** 16/10/2025 em diante
**Impacto:** **POSITIVO para operadoras**

**KM que DEIXARAM de ser glosados:**
- `km_apurada_n_licenciado` (por ar-condicionado) - antes V17
- `km_apurada_n_vistoriado` (por ar-condicionado) - antes V17
- Viagens com temperatura irregular - V17, V20

**Estimativa:** Dependente de quantas viagens eram afetadas. Pode representar **milhões de reais/mês** se a taxa de não conformidade era alta.

### V21 - Impacto Incerto

Mudanças foram revertidas. Impacto financeiro **NEUTRO**.

---

## 🎯 RECOMENDAÇÕES

### Curto Prazo

1. **Investigar V22:** Obter documentação oficial sobre a suspensão das glosas
2. **Validar Exceções:** Documentar formalmente a exceção de 10/10/2025
3. **Testar Novos Modelos:** Rodar queries de `viagem_climatizacao` para entender seu propósito

### Médio Prazo

1. **Comparação Antes/Depois V22:** Calcular diferença nos valores pagos
2. **Análise de ENEM:** Quantificar impacto das exceções de tipo de dia
3. **Auditoria de Reversões:** Entender por que mudanças foram desfeitas

### Longo Prazo

1. **Dashboard de Versões:** Criar visualização da evolução V1→V22
2. **Alertas Automáticos:** Configurar notificações para mudanças em `dbt_project.yml`
3. **Comparação com Legislação:** Mapear cada versão à base legal

---

## 📋 CHECKLIST DE AUDITORIA PÓS-MERGE

Após fazer o merge, verificar:

- [ ] `DATA_SUBSIDIO_V21_INICIO` = `2025-10-01`
- [ ] `DATA_SUBSIDIO_V22_INICIO` = `2025-10-16`
- [ ] `viagem_regularidade_temperatura.sql` tem condição `< V22`
- [ ] Modelos deprecated estão com `+enabled: false`
- [ ] Novos schemas geo, van, gps_sppo existem em `dbt_project.yml`
- [ ] CHANGELOGs de subsidio/, dashboard_subsidio_sppo_v2/ foram atualizados
- [ ] Testar compilação dbt sem erros
- [ ] Verificar se flows Prefect não quebraram

---

## 🔗 Commits por Categoria

### Subsídio (Críticos)

- `ff26edc2`: Suspensão glosa climatização (V22)
- `e44e8567`: Alteração V21 validador
- `23bd1eeb`: Reversão V21
- `77bfbbfe`: Indicador temperatura nula
- `94effd5b`: Teste tecnologia mínima
- `4e50c391`: Classificação limite viagens
- `d5bdc85e`: Equipamento aberto quando id_validador nulo
- `42bdc61d`: Revisão serviços contrato abreviado

### ENEM e Calendário

- `c7057208`: Hotfix ENEM
- `80290414`: Exceção ENEM/V+ENEM
- `4e95af66`: Exceção tipo dia ENEM
- `d39028be`: Dia atípico
- `778b6fa7`: Dia comerciário

### Operação Lago Limpo

- `1ca1a0a9`: Operação principal
- `dcc184e2`: Desabilitação modelos deprecated

### Bilhetagem

- `b314aec1`: Postgres CCT
- `3d254fec`: Teste sincronia CCT
- `b77f53b9`: Verificação captura gratuidade
- `1535af4c`: Reativa transacao_invalida
- `d3a017fe`: Alerta transação (depois desativado)

### GPS e Temperatura

- `3d84aefa`: Filtro GPS garagens
- `2481e434`: Temperatura AlertaRio
- `762dfa6e`: Remove duplicados temperatura
- `ef9a4827`: Correção aux_gps_parada

### Infraestrutura

- `62cc1b56`: Novo IP banco principal
- `49c470f3`: Novo IP Jaé
- `e03367e5`: Schedule validação Jaé

### Testes e Qualidade

- `65207cbe`: Teste vistoria/troca placa
- `7497fb99`: Teste serviço em tecnologia
- `6f71e0ea`: Teste consistência
- `bb79b282`: Teste not null validador

---

**Fim do Changelog**

_Documento gerado em: 28/11/2025_
_Análise realizada por: Claude (Auditoria SMTR/RJ)_
_Base: Comparação HEAD (`f9f4f3ff`) vs upstream/main (`c7057208`)_
