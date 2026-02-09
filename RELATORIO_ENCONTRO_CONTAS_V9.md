# Relatório de Análise: Encontro de Contas SPPO - Problemas Identificados

**Data:** 21 de Janeiro de 2026
**Repositório:** `prefeitura-rio/pipelines_rj_smtr`
**Branch/Commit:** hardcore-einstein (fork de auditoria)
**Issue Relacionada:** #1153 - Encontro de contas abr/25 a nov/25

---

## 1. Resumo Executivo

Este relatório documenta **anomalias críticas** identificadas no módulo de Encontro de Contas do sistema de cálculo de subsídios SPPO (Serviço Público de Transporte de Passageiros por Ônibus). As anomalias envolvem:

1. Mudança inexplicada de metodologia em 16/08/2024 (V9)
2. Exclusão indevida de receita tarifária real do cálculo do saldo
3. Remoção do subsídio da equação de remuneração pós-V9
4. Ausência de documentação metodológica para a versão 2.0

Estas anomalias distorcem o saldo do encontro de contas e **subestimam a remuneração total recebida pelas operadoras**.

**Relevância Jurídica:** Em 22/09/2025, os 4 Consórcios SPPO (Internorte, Santa Cruz, Intersul e Transcarioca) ajuizaram ação de encontro de contas (Processo nº 0094608-11.2025.8.19.0001) alegando divergência de R$ 126 milhões no encontro de contas (MRJ alega crédito de R$ 209 milhões; Consórcios apuram > R$ 335 milhões). O período em disputa (jun/2022 a mar/2025) coincide exatamente com as anomalias documentadas. O TJRJ reconheceu "verossimilhança na tese dos Consórcios quanto à metodologia adotada pelo MRJ" e negou efeito suspensivo ao agravo interposto pela Municipalidade.

---

## 2. Contexto: O Que é o Encontro de Contas

O Encontro de Contas é o processo de reconciliação entre:

- **Receita Tarifária Aferida**: arrecadação real das bilheterias (RDO)
- **Receita Esperada**: calculada com base na quilometragem × parâmetros tarifários
- **Subsídio Pago**: valor pago pelo município aos consórcios

### 2.1 Lógica do Sistema de Remuneração

O sistema de transporte do Rio de Janeiro opera com a seguinte lógica contratual:

```
IRK (Índice de Remuneração por Km) = Tarifa Pública + Subsídio por Km

Remuneração Total da Operadora = Receita Tarifária + Subsídio Pago
```

A **tarifa pública** não é medida em km - é um valor fixo por viagem/passageiro. O **subsídio** complementa a diferença para que a operadora receba o IRK contratado por km rodado.

O encontro de contas deve responder:

> "A soma da receita tarifária + subsídio é igual à remuneração contratual (km × IRK)?"

---

## 3. Localização dos Arquivos

| Arquivo | Caminho no Repositório |
|---------|------------------------|
| Parâmetros | `queries/models/projeto_subsidio_sppo_encontro_contas/v2/encontro_contas_parametros.sql` |
| Subsídio por serviço-dia | `queries/models/projeto_subsidio_sppo_encontro_contas/v2/encontro_contas_subsidio_sumario_servico_dia.sql` |
| RDO por serviço-dia | `queries/models/projeto_subsidio_sppo_encontro_contas/v2/staging/aux_rdo_servico_dia.sql` |
| Subsídio corrigido | `queries/models/projeto_subsidio_sppo_encontro_contas/v2/staging/aux_subsidio_servico_dia.sql` |
| Balanço RDO+Subsídio | `queries/models/projeto_subsidio_sppo_encontro_contas/v2/staging/aux_balanco_rdo_subsidio_servico_dia.sql` |
| **Cálculo do Saldo** | `queries/models/projeto_subsidio_sppo_encontro_contas/v2/staging/aux_balanco_servico_dia.sql` |

---

## 4. Anomalia 1: Mudança de Metodologia em V9 (16/08/2024)

### 4.1 O Código

Arquivo: `queries/models/projeto_subsidio_sppo_encontro_contas/v2/staging/aux_balanco_servico_dia.sql`

**Linhas 34-40 - Cálculo do Saldo:**

```sql
if(
    data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
    (
        ifnull(receita_total_aferida, 0)
        - ifnull(receita_total_esperada - subsidio_glosado, 0)
    ),
    (ifnull(receita_tarifaria_aferida, 0) - ifnull(receita_tarifaria_esperada, 0))
) as saldo
```

### 4.2 Análise Comparativa

| Período | Fórmula do Saldo | Componentes |
|---------|------------------|-------------|
| **Antes de V9** (< 16/08/2024) | `receita_total_aferida - (receita_total_esperada - subsidio_glosado)` | Considera **tarifa + subsídio** |
| **Após V9** (≥ 16/08/2024) | `receita_tarifaria_aferida - receita_tarifaria_esperada` | Considera **apenas tarifa** |

**O subsídio desaparece do cálculo do saldo pós-V9.**

### 4.3 Parâmetros Utilizados

**Linhas 46-60:**

```sql
-- Antes de V9: usa IRK (receita total)
if(b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
    b.km_apurada * par.irk,
    null
) as receita_total_esperada,

-- Sempre: usa irk_tarifa_publica (apenas tarifa)
b.km_apurada * par.irk_tarifa_publica as receita_tarifaria_esperada,

-- Antes de V9: calcula subsídio esperado
if(b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
    b.km_apurada * par.subsidio_km,
    null
) as subsidio_esperado,

-- Antes de V9: calcula glosa
if(b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
    (b.km_apurada * par.subsidio_km - valor_subsidio_pago),
    null
) as subsidio_glosado,

-- Antes de V9: soma receita total
if(b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
    ifnull(b.receita_tarifaria_aferida, 0) + ifnull(b.valor_subsidio_pago, 0),
    null
) as receita_total_aferida,

-- Antes de V9: mantém subsídio pago
if(b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
    b.valor_subsidio_pago,
    null
) as valor_subsidio_pago
```

Todos os campos relacionados ao subsídio são **virados para NULL** quando `data ≥ V9`.

---

## 5. Anomalia 2: Exclusão de POD < 80%

### 5.1 O Código

Arquivo: `queries/models/projeto_subsidio_sppo_encontro_contas/v2/staging/aux_balanco_rdo_subsidio_servico_dia.sql`

**Linhas 19-28:**

```sql
subsidio_dia_pod_80 as (
    select
        data,
        consorcio,
        servico,
        if(perc_km_planejada >= 80, km_apurada, null) as km_apurada,
        if(perc_km_planejada >= 80, valor_subsidio_pago, null) as valor_subsidio_pago,
        perc_km_planejada < 80 as indicador_pod_menor_80
    from {{ ref("aux_subsidio_servico_dia") }}
),
```

**Linhas 9-19:**

```sql
balanco_rdo_subsidio_servico_dia as (
    select
        data,
        servico,
        consorcio,
        if(
            {{ indicadores_false }}, receita_tarifaria_aferida, null
        ) as receita_tarifaria_aferida,
        if({{ indicadores_false }}, km_apurada, null) as km_apurada,
        if(
            {{ indicadores_false }}, valor_subsidio_pago, null
        ) as valor_subsidio_pago,
        ...
```

Onde `{{ indicadores_false }}` é:

```sql
not (indicador_atipico or indicador_ausencia_receita_tarifaria or indicador_data_excecao)
```

### 5.2 Impacto Prático

| Situação | KM rodados | Receita tarifária real | Subsídio pago | O que o código faz |
|----------|-----------|------------------------|---------------|-------------------|
| POD = 85% | 850 km | R$ 10.000 | R$ 20.000 | ✅ Inclui tudo |
| POD = 75% | 750 km | R$ 8.500 | R$ 0 (glosado) | ❌ **ZERA TUDO** |

Dias com POD < 80% são **completamente excluídos** do encontro de contas, incluindo a receita tarifária real que foi arrecadada.

---

## 6. Anomalia 3: Mudança de Metodologia Sem Documentação (v1 → v2)

### 6.1 Histórico de Versões

Arquivo: `queries/models/projeto_subsidio_sppo_encontro_contas/CHANGELOG.md`

```markdown
## [2.0.0] - 2025-07-15

### Adicionado

- Adicionada versão 2.0 dos modelos para o cálculo do Encontro de Contas do SPPO
  com base no Processo.Rio MTR-PRO-2025/18086
  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/511)

## [1.0.0] - 2024-05-14

### Adicionado

- Adiciona tabela de cálculo do encontro de contas por dia e serviço + agregações
  (https://github.com/prefeitura-rio/queries-rj-smtr/pull/304)
```

### 6.2 Documentação Metodológica

Arquivo: `queries/models/projeto_subsidio_sppo_encontro_contas/schema.yml`

**v1 (descontinuada em 31/12/2023):**
```yaml
description: "Balanço da receita aferida e esperada por servico e dia para
serviços-dia subsidiados. Ver metodologia em NOTA TÉCNICA TR/SUBTT Nº 01/2024"
```

**v2 (atual):**
```yaml
description: "Balanço da receita aferida e esperada por servico e dia para
pares data-serviço subsidiados [Processo.Rio MTR-PRO-2025/18086]"
```

**Nota técnica TR/SUBTT não é mais citada.** Não há documentação metodológica justificando a mudança.

### 6.3 Desabilitação da v1 para Datas ≥ V9

Todos os modelos da v1 contêm o seguinte bloqueio:

```sql
{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %}
{{ config(enabled=false) }}
{% else %}
...
```

Isso significa que **a v1 nunca funcionou para datas pós-V9**. Quando criaram a v2, ao invés de adaptar a lógica para incluir o subsídio, simplesmente o removeram.

---

## 7. Anomalia 4: Confusão Conceitual

### 7.1 O Que o Código Faz vs. Deveria Fazer

O encontro de contas atual pós-V9 é apresentado como uma "análise tarifária", mas:

1. A tarifa pública **não é medida em km** (é fixa por viagem)
2. O IRK é um **índice de remuneração total** (tarifa + subsídio)
3. O encontro de contas deveria comparar **remuneração total**, não apenas tarifa

### 7.2 Parâmetros Definidos

Arquivo: `queries/models/subsidio/valor_km_tipo_viagem.sql`

```sql
subsidio_km,          -- Valor por km de subsídio
irk,                  -- Índice de Remuneração por Quilômetro (TOTAL)
irk_tarifa_publica    -- Apenas a parte tarifária do IRK
```

Matematicamente: `IRK = irk_tarifa_publica + subsidio_km`

---

## 8. Implicações e Impactos

### 8.1 Subestimação da Receita da Operadora

Dias com POD < 80% têm sua receita tarifária **totalmente ignorada** no encontro de contas pós-V9. Isso:

- Subestima o que a operadora realmente recebeu
- Distorde o saldo anual
- Pode mascarar superávit real da operadora

### 8.2 Incoerência com a Lógica do Próprio Sistema

O sistema de subsídio já **glosa automaticamente** o km que não cumpre a meta. O `valor_subsidio_pago` já reflete as penalidades aplicadas.

**Exemplo:**
- Meta do dia: 1.000 km
- Realizado: 750 km (POD = 75%)
- Subsídio devido: R$ 0 (já glosado pelo sistema)
- Receita tarifária: R$ 8.500 (REAL)

O código atual: exclui os R$ 8.500 do encontro de contas.
O que deveria fazer: incluir R$ 8.500 de receita + R$ 0 de subsídio.

### 8.3 Distorção Temporal

| Período | Metodologia | Status |
|---------|-------------|--------|
| 2024-01-01 a 2024-08-15 | Receita total (tarifa + subsídio) | ✅ Lógica correta |
| 2024-08-16 a 2025-12-31 | Apenas tarifa, exclui POD < 80% | ❌ Lógica alterada sem justificativa |

Isso cria uma **quebra de série histórica** impossível de comparar.

---

## 9. Referências Técnicas

### 9.1 Variáveis de Versão

Arquivo: `queries/dbt_project.yml`

```yaml
DATA_SUBSIDIO_V9_INICIO: "2024-08-16"  # Apuração por faixa horária
encontro_contas_datas_v2_inicio: "2024-01-01"
```

### 9.2 Commits e Pull Requests

| PR | Descrição | Data |
|----|-----------|------|
| #304 (queries-rj-smtr) | Criação inicial do encontro de contas (v1) | 14/05/2024 |
| #511 (pipelines_rj_smtr) | Versão 2.0 - Processo.Rio MTR-PRO-2025/18086 | 15/07/2025 |

### 9.3 Modelos Relacionados a Subsídio

| Modelo | Caminho |
|--------|---------|
| Parâmetros de valor/km | `queries/models/subsidio/valor_km_tipo_viagem.sql` |
| Staging valor/km | `queries/models/subsidio/staging/staging_valor_km_tipo_viagem.sql` |
| Sumário por faixa | `queries/models/dashboard_subsidio_sppo_v2/sumario_faixa_servico_dia.sql` |
| Sumário pagamento | `queries/models/dashboard_subsidio_sppo_v2/sumario_faixa_servico_dia_pagamento.sql` |

---

## 10. Contexto Judicial: Ação de Encontro de Contas - Consórcios SPPO

### 10.1 Ação Judicial em Andamento

Em **22 de setembro de 2025**, os **quatro Consórcios do SPPO** ajuizaram ação com pedido de tutela de urgência em face do **MUNICÍPIO DO RIO DE JANEIRO** para apuração de encontro de contas.

**Documentos judiciais disponíveis:**
- `2025.09.22 - Rio Onibus - Encontro de Contas - Decisao Liminar.md`
- `AGRAVO DE INSTRUMENTO Nº 0083455-81.2025.8.19.0000.md`

**Dados do processo:**

| Campo | Informação |
|-------|------------|
| **Número** | 0094608-11.2025.8.19.0001 |
| **Distribuição** | 22/09/2025 |
| **Juíza** | Dra. Alessandra Cristina Tufvesson de Campos Melo |
| **Órgão** | 8ª Vara da Fazenda Pública da Comarca da Capital |
| **Autores** | Consórcios Internorte, Santa Cruz, Intersul e Transcarioca |
| **Réu** | Município do Rio de Janeiro |

### 10.2 Alegações dos Consórcios

A ação foi distribuída por dependência à ação nº 0072879-94.2023.8.19.0001, que firmou "segundo acordo judicial" modificando acordo anterior (ação 0045547-94.2019.8.19.0001).

**Os acordos estabelecem:**
- Novo parâmetro de remuneração: tarifa pública paga pelo usuário + subsídio pago pelo Município
- Subsídio atrelado à quantidade de quilômetros rodados
- Encontro de contas para apurar déficit ou superávit nos subsídios repassados

**Divergência de valores (período jun/2022 a mar/2025):**

| Parte | Crédito alegado |
|-------|-----------------|
| **MRJ** | Credor de **R$ 209 milhões** |
| **Consórcios** | Crédito superior a **R$ 335 milhões** |

**Principais pontos de discordância:**

1. **Valores depositados em juízo:** MRJ considerou como subsídio pago valores que foram objeto de transação e serão utilizados para compra de veículos que serão vertidos ao patrimônio do próprio MRJ

2. **Verbas de 2025:** MRJ incluiu **R$ 59,7 milhões** referentes a 2025, que só poderiam ser objeto de encontro de contas ao fim do período de 12 meses (Cláusula 6.1)

3. **Descontos unilaterais:** MRJ informou em 19/09/2025 que deduziria **R$ 5,7 milhões** do subsídio a cada quinzena, a partir de 20/09/2025, referentes a glosas consideradas indevidas

### 10.3 Decisão Liminar (1ª Instância)

A Juíza deferiu **em parte** a tutela de urgência:

```
DETERMINO que o MRJ abstenha-se de realizar a compensação ou praticar qualquer ato
que importe em cobrança dos créditos discutidos nesta ação, até que se apure o valor
efetivamente devido a título de encontro de contas, sob pena de multa equivalente a
50% do valor indevidamente retido.
```

**Pontos fundamentados:**
- Há "inegável controvérsia" na apuração do subsídio
- "Fortes indícios de inadequação da conduta do MRJ"
- MRJ considerou como crédito valores depositados em juízo ainda controvertidos
- Retenção de valores configura periculum in mora
- Pedido de restituição liminar requer maiores esclarecimentos

### 10.4 Agravo de Instrumento (2ª Instância)

**Município recorreu** (Agravo de Instrumento nº 0083455-81.2025.8.19.0000) employing strategy "throw everything at the wall":

**Alegações do Município ("jogar tudo na parede"):**

| Categoria | Argumentos |
|-----------|------------|
| **Competência** | Juízo incompetente; cláusula de encontro de contas não integra acordo homologado; violação ao juiz natural |
| **Contraditório** | Liminar sem oitiva prévia; decisão carece de fundamentação lógica |
| **Legalidade** | Ignora rito dos precatórios; afronta presunção de veracidade dos atos administrativos |
| **Mérito** | Valores de 2025 corretamente incluídos; cálculos seguiram regras válidas; valores depositados decorrem de glosas reconhecidas; atos baseados em notas técnicas TR/SUBTT 01/2024, 02/2024 e 01/2025 |
| **Econômico** | Pode provocar enriquecimento sem causa; compromete compensação financeira; afeta concessionárias deficitárias |

**Decisão da Desembargadora Relatora:**

> "**INDEFIRO** o pretendido efeito suspensivo."

**Fundamentos da negativa:**
- Verossimilhança na tese dos Consórcios quanto à metodologia adotada pelo MRJ
- Inclusão de valores judicializados com destinação específica
- Antecipação de compensação de 2025 sem encerramento do exercício anual
- Ausência de concordância sobre valores compensáveis
- Cálculos divergentes (R$ 209 mi vs R$ 335 mi) requerem prova pericial
- Questão técnica complexa que exige instrução adequada
- Periculum in mora demonstrado: glosas unilaterais podem comprometer equilíbrio econômico-financeiro
- **Não viola regime de precatórios**: não houve determinação de pagamento imediato, apenas suspensão de compensação unilateral

### 10.5 Relevância para Este Relatório

As anomalias identificadas neste relatório técnico são **altamente relevantes** para a ação judicial em curso:

1. **Período em disputa:** jun/2022 a mar/2025 - inclui exatamente o período das anomalias V9 (16/08/2024) e v2 (15/07/2025)

2. **Divergência de valores:** MRJ alega crédito de R$ 209 milhões; Consórcios apuram > R$ 335 milhões. As anomalias deste relatório podem explicar parte dessa diferença

3. **Metodologia questionada:** O próprio TJRJ reconheceu "verossimilhança na tese dos Consórcios quanto à metodologia adotada pelo MRJ". Este relatório documenta anomalias nessa metodologia

4. **Notas técnicas citadas:** O MRJ alega que seus atos "foram baseados em dados técnicos acessíveis, documentados em notas técnicas (TR/SUBTT 01/2024, 02/2024 e 01/2025)". Este relatório demonstra que após essas notas, houve mudança não justificada na fórmula de cálculo

5. **Subestimação de receita:** O código atual pode estar subdimensionando a remuneração devida aos Consórcios, impactando diretamente o apuramento do encontro de contas

6. **Exclusão de receita tarifária:** Dias com POD < 80% têm receita tarifária excluída - essa prática contabilmente é questionável e pode estar inflando artificialmente o crédito alegado pelo MRJ

**Recomendação:** As anomalias deste relatório devem ser apresentadas:
- À perícia contábil que será realizada
- Como elementos técnicos demonstrando "fortes indícios de inadequação da conduta do MRJ"
- Para fundamentar que a "metodologia adotada pelo MRJ" contém falhas que afetam o resultado do encontro de contas

---

## 11. Conclusão e Recomendações

### 11.1 Problemas Identificados

1. **Mudança de metodologia em V9 sem nota técnica justificativa**
2. **Remoção do subsídio do cálculo do saldo pós-V9**
3. **Exclusão indevida de receita tarifária de dias com POD < 80%**
4. **Quebra de série histórica sem documentação**
5. **Confusão conceitual entre análise tarifária e remuneração total**

### 11.2 Recomendações

1. **Para a ação judicial (Processo nº 0094608-11.2025.8.19.0001 - 8ª Vara da Fazenda Pública):**
   - Apresentar este relatório como elemento técnico para fundamentar a "verossimilhança na tese dos Consórcios quanto à metodologia adotada pelo MRJ"
   - Solicitar que a SMTR forneça a NOTA TÉCNICA justificando a mudança de metodologia V9 (se existir)
   - Recalcular o saldo do encontro de contas incluindo receita tarifária de dias com POD < 80%
   - Recalcular o saldo pós-V9 incluindo o subsídio pago na equação
   - Quantificar o impacto das anomalias na divergência R$ 209 mi vs R$ 335 mi

2. **Solicitar formalmente** à SMTR/CPControl a Nota Técnica que justifica a mudança de metodologia entre v1 e v2, especificamente sobre por que o subsídio deixou de compor o saldo a partir de 16/08/2024

3. **Questionar juridicamente**:
   - Por que a receita tarifária real de dias com POD < 80% é excluída do encontro de contas
   - Qual a base legal para alteração unilateral da metodologia de cálculo
   - Por que não há transparência na mudança de critérios
   - Se a mudança V9 configura alteração unilateral do equilíbrio econômico-financeiro dos contratos

4. **Revisão técnica sugerida**:

```sql
-- O que o código deveria fazer (cenário ideal):

-- Para TODOS os dias (independentemente de POD):
saldo = (receita_tarifaria_aferida + valor_subsidio_pago) - (km_apurada × IRK)

-- Onde:
-- valor_subsidio_pago JÁ inclui as glosas automaticamente
-- receita_tarifaria_aferida SEMPRE é considerada, mesmo sem subsídio
```

### 11.3 Material para Questionamento

Este relatório documenta anomalias que podem ser utilizadas em:

- **Ação judicial** nº 0094608-11.2025.8.19.0001 (4 Consórcios SPPO vs. Município do Rio de Janeiro, distribuída em 22/09/2025)
- **Perícia contábil** a ser realizada na ação acima
- Questionamentos administrativos junto à SMTR
- Recursos ao Processo.Rio MTR-PRO-2025/18086
- Demonstrar alteração unilateral de critérios que afeta o equilíbrio econômico-financeiro dos contratos
- Fundamentar "fortes indícios de inadequação da conduta do MRJ" reconhecidos pela Juíza de 1º grau

---

## 12. Histórico de Alterações deste Documento

| Data | Alteração | Autor |
|------|-----------|-------|
| 21/01/2026 | Criação inicial - Análise completa do módulo Encontro de Contas | Auditoria |
| 21/01/2026 | Adição de seção sobre contexto judicial - Ação dos 4 Consórcios SPPO | Auditoria |
| 21/01/2026 | Atualização com documentos judiciais corretos e análise do Agravo de Instrumento | Auditoria |

---

**Fim do Relatório**
