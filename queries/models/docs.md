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

{% docs tipo_dia %}
Dia da semana - categorias: Dia Útil, Sábado, Domingo
{% enddocs %}

{% docs faixa_horaria_inicio %}
Horário inicial da faixa horária
{% enddocs %}

{% docs faixa_horaria_fim %}
Horário final da faixa horária
{% enddocs %}

{% docs partidas %}
Quantidade de partidas planejadas
{% enddocs %}

{% docs quilometragem %}
Quilometragem planejada
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

{% docs inicio_periodo %}
Início do período de operação planejado
{% enddocs %}

{% docs fim_periodo %}
Fim do período de operação planejado
{% enddocs %}

{% docs feed_start_date %}
(Partição) Data inicial do feed (versão).
{% enddocs %}

{% docs feed_version %}
String que indica a versão atual do conjunto de dados GTFS.
{% enddocs %}

{% docs linha %}
Número da Linha
{% enddocs %}

{% docs tipo_servico %}
Tipo de Serviço da Linha
{% enddocs %}

{% docs ordem_servico %}
Ordem de Serviço da Linha
{% enddocs %}

{% docs codigo_veiculo %}
Código do Tipo de Veículo do Serviço
{% enddocs %}

{% docs tarifa_codigo %}
Tarifa determinada
{% enddocs %}

{% docs tarifa_valor %}
Tarifa Praticada
{% enddocs %}

{% docs frota_determinada %}
Frota Determinada
{% enddocs %}

{% docs frota_licenciada %}
Frota Licenciada para a linha
{% enddocs %}

{% docs frota_operante %}
Frota Operacional do dia
{% enddocs %}

{% docs qtd_viagens %}
Número de Viagens da Linha no dia
{% enddocs %}

{% docs qtd_km_cobertos %}
Quilometragem Coberta
{% enddocs %}

{% docs qtd_grt_idoso %}
Gratuidades Transportadas - Idosos
{% enddocs %}

{% docs qtd_grt_especial %}
Gratuidades Transportadas - Portadores de Necessidades Especiais
{% enddocs %}

{% docs qtd_grt_estud_federal %}
Gratuidades Transportadas - Estudantes da União
{% enddocs %}

{% docs qtd_grt_estud_estadual %}
Gratuidades Transportadas - Estudantes do Estado
{% enddocs %}

{% docs qtd_grt_estud_municipal %}
Gratuidades Transportadas - Estudantes do Município
{% enddocs %}

{% docs qtd_grt_rodoviario %}
Gratuidades Transportadas - Funcionários das Empresas
{% enddocs %}

{% docs qtd_buc_1_perna %}
Bilhete único Carioca (1ª Perna)
{% enddocs %}

{% docs qtd_buc_2_perna_integracao %}
Bilhete Único Carioca (2ª Perna)
{% enddocs %}

{% docs receita_buc %}
Receita de Bilhete Único Carioca
{% enddocs %}

{% docs qtd_buc_supervia_1_perna %}
Bilhete único Carioca - Supervia (1ª Perna)
{% enddocs %}

{% docs qtd_buc_supervia_2_perna_integracao %}
Bilhete Único Carioca - Supervia (2ª Perna)
{% enddocs %}

{% docs receita_buc_supervia %}
Receita de Bilhete Único Carioca - Supervia
{% enddocs %}

{% docs qtd_cartoes_perna_unica_e_demais %}
Cartões Perna Única e demais transações valoradas não contempladas nos campos deste RDO
{% enddocs %}

{% docs receita_cartoes_perna_unica_e_demais %}
Receita Cartões Perna Única e Receita das demais transações valoradas não contempladas nos campos deste RDO
{% enddocs %}

{% docs qtd_pagamentos_especie %}
Passageiros Pagantes em Espécie
{% enddocs %}

{% docs receita_especie %}
Receita Passageiros Pagantes em Espécie
{% enddocs %}

{% docs qtd_grt_passe_livre_universitario %}
Gratuidades Transportadas - Passe Livre Universitário
{% enddocs %}

{% docs class_servico %}
Classificação do Serviço: Diretão, Estação do BRT, Rodoviário, Troncal do BRT, Urbano.
{% enddocs %}

{% docs qtd_passageiros_total %}
Quantidade total de passageiros
{% enddocs %}

{% docs codigo %}
Número de ordem do veículo.
{% enddocs %}

{% docs trajeto %}
Nome do trajeto.
{% enddocs %}

{% docs sentido %}
Sentido da linha
{% enddocs %}

{% docs velocidade_media %}
Velocidade média da viagem (km/h)
{% enddocs %}