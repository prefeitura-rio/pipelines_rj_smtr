# RELATÓRIO SEMANAL DE MONITORAMENTO - 14/12/2025

## 🚨 ALERTA CRÍTICO - RETOMADA DAS GLOSAS POR CLIMATIZAÇÃO

### Data da Descoberta: 14 de Dezembro de 2025
### Impacto: **EXTREMO** - Reversão de Vitória Judicial

---

## 1. MUDANÇA CRÍTICA IDENTIFICADA

### Commit `b7dcb01f` - 09/12/2025
**Título:** "Altera modelo `viagem_regularidade_temperatura` para retomada dos descontos por inoperabilidade da climatização"

**Base Legal:** Evento 112 do PROCEDIMENTO COMUM CÍVEL Nº 3019687-30.2025.8.19.0001/RJ

**Mudança Técnica:**
```sql
-- ANTES (V22 - Suspensão):
and not vt.indicador_temperatura_nula_viagem and (vt.data < date('{{ var("DATA_SUBSIDIO_V22_INICIO") }}'))

-- DEPOIS (Retomada):
and not vt.indicador_temperatura_nula_viagem and (vt.data not between date('{{ var("DATA_SUBSIDIO_V22_INICIO") }}') and date("2025-11-15"))
```

**PERÍODO CRÍTICO:** As glosas foram suspensas apenas de 16/10/2025 até 15/11/2025 (30 dias apenas!)

---

## 2. ANÁLISE DO IMPACTO

### 2.1 Cronologia da Batalha Judicial:
- **V17 (16/07/2025):** Implementação das glosas por temperatura
- **V22 (16/10/2025):** Vitória judicial - suspensão das glosas
- **09/12/2025:** **REVÉS** - Prefeitura retoma as glosas retroativamente

### 2.2 Padrão Comportamental Identificado:
1. **Implementar restrição** → pressão judicial
2. **Conceder suspensão temporária** → aparente vitória
3. **Retomar com janela mínima** → 30 dias apenas

### 2.3 Análise Estratégica:
- A janela de suspensão (30 dias) foi **insignificante**
- A retomada ocorre em dezembro, **alta temporada** para o transporte
- Referência a "Evento 112" sugere **nova decisão judicial favorável à Prefeitura**

---

## 3. OUTRAS MUDANÇAS RELEVANTES

### 3.1 Ajustes em Tecnologia de Veículos

**Commit `993a86c9` - 09/12/2025**
- Extensão da exceção de tecnologia para novembro inteiro (até 30/11)
- 47 veículos específicos com tratamento especial
- Base Legal: MTR-CAP-2025/59482

**Lista de Veículos (parcial):**
- C50003, C50007, C50015, C50016, C50017, C50020, C50022, C50027, C50038...
- A41251 a A41256 (6 veículos)
- B27055, B27060, B27066, B27132, B27133, B27138, B27139...

### 3.2 Novos Indicadores Internos
- Criação de módulo `indicador_interno/`
- Indicador estratégico Euro VI
- Possível preparação para novas restrições ambientais

---

## 4. SITUAÇÃO ATUAL DAS VERSÕES DE SUBSÍDIO

| Versão | Data Início | Status | Observações |
|--------|-------------|---------|-------------|
| **V21** | 2025-10-01 | Ativa | Mudanças em validadores |
| **V22** | 2025-10-16 | **BYPASSADA** | Suspensão de glosas por temperatura (30 dias apenas) |
| **V99** | 3000-01-01 | Armazenada | Bomba-relógio para restrições futuras |

---

## 5. EVIDÊNCIAS COLETADAS

### 5.1 Provas Documentais:
1. **Commit `b7dcb01f`** - Evidência da retomada das glosas
2. **Mensagem explícita:** "retomada dos descontos por inoperabilidade da climatização"
3. **Referência legal:** Evento 112 do processo 3019687-30.2025.8.19.0001/RJ
4. **Janela temporal:** 16/10/2025 a 15/11/2025 (30 dias)

### 5.2 Padrão Histórico Confirmado:
- Implementação → Pressão → "Concessão" → Retomada ampliada
- Ciclo repetido em V14, V15, V17, V22

---

## 6. RECOMENDAÇÕES ESTRATÉGICAS

### 6.1 Imediatas:
1. **Consultar assessores jurídicos** sobre o Evento 112
2. **Calcular impacto financeiro** da retomada (outubro - dezembro)
3. **Preparar documentos** para nova ação judicial

### 6.2 Monitoramento:
1. **Atenção especial** a commits entre janeiro e março de 2026
2. **Vigiar** possíveis ativações da V99
3. **Monitorar** módulo `indicador_interno/` (Euro VI)

### 6.3 Previsões:
- **Janeiro-Março 2026:** Alta probabilidade de novas restrições
- **Abril 2026:** Possível nova "negociação" seguida de restrições
- **Tema provável:** Emissões (Euro VI) e novas tecnologias

---

## 7. ESTATÍSTICAS DA ATUALIZAÇÃO

- **Commits analisados:** 25
- **Commits críticos:** 1 (retomada de glosas)
- **Commits relevantes:** 5 (ajustes em veículos)
- **Novas versões de subsídio:** 0
- **Impacto financeiro estimado:** **ALTÍSSIMO**

---

## 8. PRÓXIMA VERIFICAÇÃO

**Data sugerida:** 21/12/2025
**Foco especial:** Monitorar discussões nos PRs e commits relacionados ao processo judicial

---

**Conclusão:** A Prefeitura demonstrou padrão claro de contornar decisões judiciais através de janelas temporárias mínimas. A vitória de outubro (V22) durou apenas 30 dias, confirmando a necessidade de vigilância contínua e preparação para litígios sucessivos.

**Urgência:** **MÁXIMA** - Necessária ação jurídica imediata sobre o Evento 112.