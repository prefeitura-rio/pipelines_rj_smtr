{% docs consorcio %}
Consórcio ao qual o serviço pertence.
{% enddocs %}

{% docs id_veiculo %}
Código identificador do veículo (número de ordem).
{% enddocs %}

{% docs datetime_ultima_atualizacao %}
Última atualização (GMT-3).
{% enddocs %}

{% docs datetime_partida %}
Horário de início da viagem.
{% enddocs %}

{% docs datetime_chegada %}
Horário de fim da viagem.
{% enddocs %}

{% docs distancia_planejada %}
Distância do shape (trajeto) planejado (km).
{% enddocs %}

{% docs tipo_viagem_status %}
Classificação do tipo de viagem.
{% enddocs %}

{% docs servico %}
Serviço realizado pelo veículo.
{% enddocs %}

{% docs id_viagem %}
Código único identificador da viagem.
{% enddocs %}

{% docs project_id %}
Nome do projeto (rj-smtr)
{% enddocs %}

{% docs dataset_id %}
Nome do conjunto de dados
{% enddocs %}

{% docs table_id %}
Nome da tabela
{% enddocs %}

{% docs column_name %}
Nome da coluna
{% enddocs %}

{% docs data_type %}
Tipo de dado da coluna
{% enddocs %}

{% docs metadado_descricao %}
Descrição da coluna
{% enddocs %}

{% docs timestamp_gps %}
Timestamp de emissão do sinal de GPS
{% enddocs %}

{% docs data_gps %}
Data do timestamp de emissão do sinal de GPS
{% enddocs %}

{% docs hora_gps %}
Hora do timestamp de emissão do sinal de GPS
{% enddocs %}

{% docs latitude_gps %}
Parte da coordenada geográfica (eixo y) em graus decimais (EPSG:4326 - WGS84)
{% enddocs %}

{% docs longitude_gps %}
Parte da coordenada geográfica (eixo x) em graus decimais (EPSG:4326 - WGS84)
{% enddocs %}

{% docs flag_em_movimento %}
Veículos com 'velocidade' abaixo da 'velocidade_limiar_parado', são considerados como parado (false). Caso contrário, são considerados andando (true)
{% enddocs %}

{% docs flag_trajeto_correto_hist %}
Flag de verificação se, nos últimos 'intervalo_max_desvio_segundos', ao menos algum ponto de GPS encontra-se até o limite de 'tamanho_buffer_metros' do shape da linha (true). Se não estiver, retorna false.
{% enddocs %}

{% docs flag_em_operacao %}
Veículos com as flags 'flag_em_movimento' e 'flag_trajeto_correto_hist' com valor true são considerados como em operação.
{% enddocs %}

{% docs tipo_parada %}
Identifica veículos parados em terminais ou garagens.
{% enddocs %}

{% docs flag_linha_existe_sigmob %}
Flag de verificação se a linha informada existe no SIGMOB.
{% enddocs %}

{% docs flag_trajeto_correto %}
Flag de verificação se o ponto de GPS encontra-se até o limite de 'tamanho_buffer_metros' do shape da linha (true). Se não estiver, retorna false.
{% enddocs %}

{% docs status_veiculo_gps %}
"Em Operação": Quando 'flag_em_movimento' é true e 'flag_trajeto_correto_hist' é true.
"Operando fora do trajeto": Quando 'flag_em_movimento' é true e 'flag_trajeto_correto_hist' é false.
"Parado": Quando 'flag_em_movimento' é false:
 - Se 'tipo_parada' não é nulo, o veículo é considerado "Parado" seguido pelo tipo de parada (ex.: "Parado terminal").
 - Se 'tipo_parada' é nulo:
    - Se 'flag_trajeto_correto_hist' é true, o status é "Parado trajeto correto".
    - Se 'flag_trajeto_correto_hist' é false, o status é "Parado fora trajeto".
{% enddocs %}

{% docs velocidade_instantanea %}
Velocidade instantânea do veículo, conforme informado pelo GPS (km/h)
{% enddocs %}

{% docs velocidade_estimada_10_min %}
Velocidade média nos últimos 10 minutos de operação (km/h)
{% enddocs %}

{% docs distancia_gps %}
Distância da última posição do GPS em relação à posição atual (m)
{% enddocs %}

{% docs versao %}
Código de controle de versão do dado (SHA Github)
{% enddocs %}

{% docs id_validador %}
Número de série do validador
{% enddocs %}

{% docs quantidade_total_transacao %}
Quantidade total de transações realizadas
{% enddocs %}

{% docs tipo_gratuidade %}
Tipo da gratuidade (Estudante, PCD, Sênior)
{% enddocs %}

{% docs tipo_pagamento %}
Tipo de pagamento utilizado
{% enddocs %}

{% docs timestamp_captura %}
Timestamp de captura pela SMTR
{% enddocs %}

{% docs data_versao %}
Data de referência do planejamento (versão).
{% enddocs %}

{% docs content %}
Dados brutos capturados aninhados em formato JSON
{% enddocs %}

{% docs modo %}
Tipo de transporte (Ônibus, Van, BRT)
{% enddocs %}

{% docs vista %}
Itinerário do serviço (ex: Bananal ↔ Saens Peña)
{% enddocs %}

{% docs viagens_planejadas %}
Viagens planejadas no período
{% enddocs %}

{% docs tipo_dia %}
Dia da semana considerado para o cálculo da distância planejada - categorias: Dia Útil, Sabado, Domingo
{% enddocs %}

{% docs inicio_periodo %}
Início do período de operação planejado
{% enddocs %}

{% docs fim_periodo %}
Fim do período de operação planejado
{% enddocs %}

{% docs feed_start_date %}
(Partição) Data inicial do feed (versão).
{% enddocs %}