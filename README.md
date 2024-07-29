# Pipelines rj-smtr


## Setup

### Etapa 1 - Preparação de Ambiente

- Na raiz do projeto, crie um ambiente virtual para isolar as dependencias:
    - `python3.10 -m venv .pipelines`

- Ative o ambiente virtual:
    - `. .pipelines/bin/activate`

- Instale as dependencias do projeto:
    - `poetry install --all-extras`
    - `pip install -e .`

- Crie um arquivo `.env` na raiz do projeto, contendo as seguintes variáveis:
    - ```
      INFISICAL_ADDRESS = ''
      INFISICAL_TOKEN = ''

    - Solicite os valores a serem utilizados para a equipe de devops

- Adicione as variáveis de ambiente à sua sessão de terminal:
    - `set -a && source .env && set +a`

### Testando flows localmente:

- Adicione um arquivo `test.py` na raiz do projeto:
    - Neste arquivo, você deve importar o flow a ser testado
    - Importar a função `run_local`
        - `from pipelines.utils.prefect import run_local`
        - A assinatura da função é a seguinte:
            `run_local(flow: prefect.Flow, parameters: Dict[str, Any] = None)`
            Permitindo que se varie os parâmetros a serem passados ao flow durante
            uma execução local
    - Use `run_local(<flow_a_ser_testado>)` e execute o arquivo:
        - `python test.py`
        - Uma dica interessante que pode ajudar no processo de teste e debug é adicionar
            `| tee logs.txt`
          ao executar seu teste.
        - Isso gerará um arquivo com os logs daquela execução, para que você possa
        analisar esses logs mais facilmente do que usando somente o terminal.

### Etapa 2 - Deploy para staging e PR

- Sempre trabalhe com branchs `staging/<nome>`
- Dê push e abra seu Pull Request.
- Cada commit nesta branch irá disparar as rotinas do Github que:
- Verificam formatação
- Fazem Deploy
- Registram flows em staging (ambiente de testes)
- Você acompanha o status destas rotinas na própria página do seu PR
- Flows registrados aparecem no servidor Prefect. Eles podem ser rodados por lá
