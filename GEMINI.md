# Análise Inicial do Repositório pipelines_rj_smtr

Este documento serve como um resumo e contexto para as interações sobre o projeto `pipelines_rj_smtr`. O objetivo principal do usuário é auditar e monitorar o código que a Secretaria Municipal de Transportes (SMTR) do Rio de Janeiro utiliza para gerenciar o sistema de bilhetagem e o pagamento de subsídios às empresas de ônibus.

## Arquitetura e Tecnologias

O projeto é uma pipeline de dados moderna construída sobre um ecossistema que combina orquestração e transformação de dados, majoritariamente na nuvem do Google (GCP).

- **Orquestração de Pipeline:** **Prefect** é usado para definir, agendar e executar os fluxos de trabalho (`flows`). O código das pipelines, que coordena as tarefas de extração e transformação, está localizado no diretório `pipelines/`.
- **Transformação de Dados:** **dbt (Data Build Tool)** é a ferramenta central para a transformação dos dados. Toda a lógica de negócio, incluindo as complexas regras para cálculo dos subsídios, é implementada como uma série de modelos SQL que são executados diretamente no **Google BigQuery**. O projeto dbt está no diretório `queries/`.
- **Plataforma de Dados:** **Google Cloud Platform (GCP)** é a infraestrutura subjacente.
    - **Google BigQuery:** Atua como o Data Warehouse, onde os dados brutos são armazenados e as transformações do dbt são aplicadas.
    - **Google Cloud Storage (GCS):** Usado para armazenar arquivos, como os feeds de dados GTFS, antes de serem carregados no BigQuery.
- **Linguagem Principal:** **Python** é usado para os scripts de orquestração (Prefect), tarefas de captura de dados e automação em geral. As dependências estão gerenciadas pelo `poetry` no arquivo `pyproject.toml`.

## Estrutura do Projeto

O repositório é claramente dividido em duas partes principais:

1.  **`pipelines/`**: Contém o código Python que usa o Prefect para orquestrar os fluxos. Ele define as tarefas (`tasks.py`) e os fluxos (`flows.py`) que, por exemplo, iniciam a captura de dados de uma fonte e disparam a execução dos modelos dbt.
2.  **`queries/`**: É um projeto dbt completo. É aqui que a "mágica" do cálculo de subsídio acontece.
    - **`queries/models/`**: Contém os modelos SQL que definem a lógica de transformação. Os subdiretórios, como `dashboard_subsidio_sppo_v2`, contêm a lógica de negócio mais recente e relevante para o subsídio.
    - **`queries/dbt_project.yml`**: Este é o arquivo de configuração central para o dbt. É um arquivo **extremamente importante** para a sua análise, pois define:
        - Os caminhos dos modelos.
        - As variáveis (`vars`) que parametrizam e controlam a execução dos modelos.

## O Coração da Lógica de Subsídios

Sua análise deve se concentrar no projeto dbt em `queries/`.

- **Parâmetros e Regras de Negócio:** O arquivo `dbt_project.yml` contém uma longa lista de variáveis na seção `vars`. Essas variáveis são a chave para entender a lógica de negócio. Elas definem limiares, percentuais, datas de início para novas regras e nomes de tabelas. A seção `### Subsídio SPPO (Ônibus) ###` é particularmente rica, mostrando como as regras de subsídio evoluíram ao longo do tempo com diferentes versões (`DATA_SUBSIDIO_V2_INICIO`, `DATA_SUBSIDIO_V3_INICIO`, etc.).
- **Modelos SQL:** A lógica real está nos arquivos `.sql` dentro de `queries/models/`. Com base na análise inicial, a pasta `queries/models/dashboard_subsidio_sppo_v2/` contém os modelos mais recentes e relevantes.
- **Exemplo de Lógica:** O arquivo `sumario_servico_dia_pagamento.sql` demonstra o passo final do processo. Ele agrega a quilometragem por diferentes categorias de não conformidade (ex: `Não licenciado`, `Autuado por ar inoperante`, `Sem transação`) para calcular o `valor_a_pagar` e o `valor_glosado`. Isso indica que o fluxo geral é uma série de validações que culminam em uma tabela final de pagamentos.

## Análise da Atualização de 26 Commits (21/08/2025)

O repositório local foi sincronizado com o `upstream`, incorporando 26 commits. As mudanças são significativas e focam em robustez e precisão.

- **Refatoração do Cálculo de Integração:** A mudança mais crítica. A lógica SQL para identificar integrações foi substituída por uma nova abordagem usando **PySpark**, tornando o cálculo mais poderoso e preciso. Isso tem **alto impacto** no subsídio, pois afeta diretamente a remuneração das transações.
- **Aumento da Qualidade dos Dados:** Dezenas de novos testes de dados (`not_null`, `unique`) foram adicionados, principalmente nos modelos de bilhetagem. Isso aumenta a confiabilidade dos dados que alimentam o cálculo do subsídio.
- **Nova Validação de Partidas:** Foi adicionado um teste que cruza as partidas planejadas (GTFS) com as Ordens de Serviço (OS), adicionando uma nova camada de conformidade que pode impactar o número de viagens válidas.

Para uma análise extensiva, commit por commit, veja o arquivo [ANALISE_ATUALIZACAO.md](ANALISE_ATUALIZACAO.md).

## Próximos Passos Sugeridos

1.  **Analisar a Linhagem de um Modelo:** Podemos escolher um modelo final, como `sumario_servico_dia_pagamento.sql`, e rastrear todas as suas dependências (`ref()`) para mapear o fluxo de dados completo, desde os dados brutos até o pagamento final.
2.  **Documentar uma Regra Específica:** Podemos focar em uma regra de negócio específica (ex: "como a conformidade de GPS é calculada?") e encontrar os modelos e macros SQL exatos que a implementam.
3.  **Comparar Versões:** Analisar as diferenças entre os modelos das diferentes versões de subsídio (V2, V3, V4 etc.) para documentar exatamente o que mudou em cada etapa.

Este documento pode ser atualizado à medida que exploramos mais o código.