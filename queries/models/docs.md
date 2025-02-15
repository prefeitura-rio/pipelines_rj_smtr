{% docs consorcio %}
Consórcio ao qual o serviço pertence
{% enddocs %}

{% docs id_veiculo %}
Código identificador do veículo [número de ordem]
{% enddocs %}

{% docs datetime_ultima_atualizacao %}
Última atualização [GMT-3]
{% enddocs %}

{% docs datetime_partida %}
Horário de início da viagem
{% enddocs %}

{% docs datetime_chegada %}
Horário de fim da viagem
{% enddocs %}

{% docs distancia_planejada %}
Distância do shape [trajeto] planejado (km)
{% enddocs %}

{% docs tipo_viagem_status %}
Classificação do tipo de viagem
{% enddocs %}

{% docs servico %}
Serviço realizado pelo veículo
{% enddocs %}

{% docs id_viagem %}
Código único identificador da viagem
{% enddocs %}

{% docs project_id %}
Nome do projeto [rj-smtr]
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
Parte da coordenada geográfica [eixo y] em graus decimais [EPSG:4326 - WGS84]
{% enddocs %}

{% docs longitude_gps %}
Parte da coordenada geográfica [eixo x] em graus decimais [EPSG:4326 - WGS84]
{% enddocs %}

{% docs flag_em_movimento %}
Veículos com 'velocidade' abaixo da 'velocidade_limiar_parado', são considerados como parado [false]. Caso contrário, são considerados andando [true]
{% enddocs %}

{% docs flag_trajeto_correto_hist %}
Flag de verificação se, nos últimos 'intervalo_max_desvio_segundos', ao menos algum ponto de GPS encontra-se até o limite de 'tamanho_buffer_metros' do shape da linha [true]. Se não estiver, retorna false
{% enddocs %}

{% docs flag_em_operacao %}
Veículos com as flags 'flag_em_movimento' e 'flag_trajeto_correto_hist' com valor true são considerados como em operação
{% enddocs %}

{% docs tipo_parada %}
Identifica veículos parados em terminais ou garagens
{% enddocs %}

{% docs flag_linha_existe_sigmob %}
Flag de verificação se a linha informada existe no SIGMOB
{% enddocs %}

{% docs flag_trajeto_correto %}
Flag de verificação se o ponto de GPS encontra-se até o limite de 'tamanho_buffer_metros' do shape da linha [true]. Se não estiver, retorna false
{% enddocs %}

{% docs status_veiculo_gps %}
"Em Operação": Quando 'flag_em_movimento' é true e 'flag_trajeto_correto_hist' é true
"Operando fora do trajeto": Quando 'flag_em_movimento' é true e 'flag_trajeto_correto_hist' é false
"Parado": Quando 'flag_em_movimento' é false:
 - Se 'tipo_parada' não é nulo, o veículo é considerado "Parado" seguido pelo tipo de parada [ex.: "Parado terminal"]
 - Se 'tipo_parada' é nulo:
    - Se 'flag_trajeto_correto_hist' é true, o status é "Parado trajeto correto"
    - Se 'flag_trajeto_correto_hist' é false, o status é "Parado fora trajeto"
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
Código de controle de versão do dado [SHA Github]
{% enddocs %}

{% docs id_validador %}
Número de série do validador
{% enddocs %}

{% docs quantidade_total_transacao %}
Quantidade total de transações realizadas
{% enddocs %}

{% docs tipo_gratuidade %}
Tipo da gratuidade [Estudante, PCD, Sênior]
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
Data de referência do planejamento [versão]
{% enddocs %}

{% docs content %}
Dados brutos capturados aninhados em formato JSON
{% enddocs %}

{% docs modo %}
Tipo de transporte [Ônibus, Van, BRT]
{% enddocs %}

{% docs vista %}
Itinerário do serviço [ex: Bananal ↔ Saens Peña]
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
Data inicial do feed [versão] [partição]
{% enddocs %}

{% docs feed_version %}
String que indica a versão atual do conjunto de dados GTFS
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
Bilhete único Carioca [1ª Perna]
{% enddocs %}

{% docs qtd_buc_2_perna_integracao %}
Bilhete Único Carioca [2ª Perna]
{% enddocs %}

{% docs receita_buc %}
Receita de Bilhete Único Carioca
{% enddocs %}

{% docs qtd_buc_supervia_1_perna %}
Bilhete único Carioca - Supervia [1ª Perna]
{% enddocs %}

{% docs qtd_buc_supervia_2_perna_integracao %}
Bilhete Único Carioca - Supervia [2ª Perna]
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
Classificação do Serviço: Diretão, Estação do BRT, Rodoviário, Troncal do BRT, Urbano
{% enddocs %}

{% docs qtd_passageiros_total %}
Quantidade total de passageiros
{% enddocs %}

{% docs codigo %}
Número de ordem do veículo
{% enddocs %}

{% docs trajeto %}
Nome do trajeto
{% enddocs %}

{% docs sentido %}
Sentido da linha
{% enddocs %}

{% docs indicador_viagem_dentro_limite %}
Indica se a viagem foi remunerada por estar abaixo do teto de 120%/200%
{% enddocs %}

{% docs indicador_penalidade_judicial %}
Indica que o valor encontra-se sob julgamento de ação judicial e será depositado em juízo
{% enddocs %}

{% docs tipo_os %}
Tipo de Ordem de Serviço [ex: 'Regular', 'Extraordinária - Verão']
{% enddocs %}

{% docs data_inicio_matriz %}
Data de inicio da versão da matriz de integração
{% enddocs %}

{% docs tecnologia %}
Tecnologia utilizada no veículo [BASICO, MIDI, MINI, PADRON, ARTICULADO]
{% enddocs %}

{% docs tecnologia_remunerada %}
Tecnologia remunerada conforme legislação em vigor
{% enddocs %}

{% docs pof %}
Indicador percentual de quilometragem apurada em relação à planejada do serviço por faixa horária [não inclui veículos não licenciados e não vistoriados a partir de 2024-09-01]
{% enddocs %}

{% docs valor_glosado_tecnologia %}
Valor da diferença entre a tecnologia remunerada e tecnologia apurada
{% enddocs %}

{% docs data_captura %}
Data da captura
{% enddocs %}

{% docs ano_fabricacao %}
Ano de fabricação do veículo
{% enddocs %}

{% docs carroceria %}
Descrição do modelo da carroceria
{% enddocs %}

{% docs data_ultima_vistoria %}
Data da última vistoria do veículo
{% enddocs %}

{% docs id_carroceria %}
Código do modelo da carroceria
{% enddocs %}

{% docs id_chassi %}
Código do modelo do chassi
{% enddocs %}

{% docs id_fabricante_chassi %}
Identificador do fabricante do chassi
{% enddocs %}

{% docs id_interno_carroceria %}
Código interno do modelo de carroceria
{% enddocs %}

{% docs id_planta %}
Código da planta do veículo
{% enddocs %}

{% docs indicador_ar_condicionado %}
Indicador se possui ar condicionado [Verdadeiro/Falso]
{% enddocs %}

{% docs indicador_elevador %}
Indicador se possui elevador [Verdadeiro/Falso]
{% enddocs %}

{% docs indicador_usb %}
Indicador se tem USB [Verdadeiro/Falso]
{% enddocs %}

{% docs indicador_wifi %}
Indicador se tem Wi-fi [Verdadeiro/Falso]
{% enddocs %}

{% docs nome_chassi %}
Descrição do modelo do chassi
{% enddocs %}

{% docs permissao %}
Número da permissão da operadora ao qual o veículo está vinculado no STU
{% enddocs %}

{% docs placa %}
Placa do veículo
{% enddocs %}

{% docs quantidade_lotacao_pe %}
Capacidade de passageiros em pé
{% enddocs %}

{% docs quantidade_lotacao_sentado %}
Capacidade de passageiros sentados
{% enddocs %}

{% docs tipo_combustivel %}
Tipo de combustível utilizado
{% enddocs %}

{% docs tipo_veiculo %}
Tipo de veículo
{% enddocs %}

{% docs status %}
Licenciado - Veículo licenciado no STU
Válido - Veículo com solicitação válida para ingresso no STU
{% enddocs %}

{% docs ano_ultima_vistoria_atualizado %}
Ano atualizado da última vistoria realizada pelo veículo
{% enddocs %}

{% docs data_inicio_vinculo %}
Data de início do vínculo do veículo no STU
{% enddocs %}

{% docs modo_brt %}
BRT – nesta tabela consta apenas este modo
{% enddocs %}

{% docs modo_sppo %}
SPPO – nesta tabela consta apenas este modo
{% enddocs %}

{% docs modo_onibus %}
ÔNIBUS – nesta tabela consta apenas este modo
{% enddocs %}

{% docs linha %}
Serviço de ônibus [linha] ou, se realocada, informada pela empresa operadora
{% enddocs %}

{% docs data_autuacao %}
Data da autuação
{% enddocs %}

{% docs data_transacao %}
Data da transação
{% enddocs %}

{% docs data_viagem %}
Data da transação
{% enddocs %}

{% docs data %}
Data
{% enddocs %}

{% docs data_agente %}
Data de registro pelo agente público
{% enddocs %}

{% docs data_infracao %}
Data da infração
{% enddocs %}

{% docs data_inicio_parametros %}
Data inicial do período de vigência dos demais atributos
{% enddocs %}

{% docs data_limite_recurso %}
Data limite para recurso em primeira instância
{% enddocs %}

{% docs datetime_realocacao %}
Datetime_realocacao que o registro da realocação foi informado pela empresa operadora
{% enddocs %}

{% docs descricao_autuador %}
Descrição da unidade de autuação
{% enddocs %}

{% docs descricao_situacao_autuacao %}
Descrição da situação da autuação
{% enddocs %}

{% docs descricao_servico_jae %}
Nome longo da linha operada pelo veículo com variação de serviço [ex: 010, 011SN, ...] ou nome da estação de BRT na Jaé
{% enddocs %}

{% docs extensao_ida %}
Distância percorrida na ida
{% enddocs %}

{% docs id_auto_infracao %}
Código do auto de infração
{% enddocs %}

{% docs id_consorcio %}
Identificador do consórcio na tabela cadastro.consorcios
{% enddocs %}

{% docs id_registro %}
ID do registro [HASH SHA256]
{% enddocs %}

{% docs indicador_autuacao_limpeza %}
Indicador se o veículo foi autuado por infração relacionada à limpeza do veículo
{% enddocs %}

{% docs indicador_licenciado %}
Indicador se o veículo encontra-se licenciado
{% enddocs %}

{% docs km_apurada_autuado_ar_inoperante %}
Quilometragem apurada de viagens de veículos autuados por ar inoperante
{% enddocs %}

{% docs km_apurada_licenciado_sem_ar_n_autuado %}
Quilometragem apurada de viagens de veículos sem ar e não autuados
{% enddocs %}

{% docs km_apurada_n_licenciado %}
Quilometragem apurada de viagens de veículos não licenciados
{% enddocs %}

{% docs km_apurada_registrado_com_ar_inoperante %}
Distância apurada de viagens realizadas por veículo licenciado com ar condicionado e registrado por agente de verão [RESOLUÇÃO SMTR Nº 3.682/2024] em razão de inoperância ou mau funcionamento deste (km)
{% enddocs %}

{% docs link_foto %}
Link com a imagem interna do veículo
{% enddocs %}

{% docs longitude_transacao %}
Longitude da transação [WGS84]
{% enddocs %}

{% docs perc_conformidade_registros %}
Percentual de minutos da viagem com registro de sinal de GPS
{% enddocs %}

{% docs percentual_rateio %}
Percentual de rateio do valor total para a operadora
{% enddocs %}

{% docs quantidade_transacao_especie %}
Quantidade de transações feitas em espécie
{% enddocs %}

{% docs recurso_penalidade_multa %}
Número do processo de recurso contra aplicação de penalidade de multa em primeira instância
{% enddocs %}

{% docs shape %}
Shape em formato geográfico [LineString]
{% enddocs %}

{% docs shape_id %}
Código identificador do shape [trajeto]
{% enddocs %}

{% docs status_infracao %}
Descrição do status da infração
{% enddocs %}

{% docs tipificacao_resumida %}
Descrição da autuação
{% enddocs %}

{% docs trip_id_planejado %}
Código identificador de trip de referência no GTFS
{% enddocs %}

{% docs validacao %}
Coluna de validação do registro enviado pelo agente público [apenas true nesta tabela]
{% enddocs %}

{% docs valor_a_pagar %}
Valor efetivo de pagamento [valor_total_apurado - valor_acima_limite - valor_glosado]
{% enddocs %}

{% docs valor_acima_limite %}
Valor apurado das viagens que não foram remuneradas por estar acima do teto de 120% / 200%
{% enddocs %}

{% docs valor_pago %}
Valor pago da autuação (R$)
{% enddocs %}

{% docs valor_total_glosado %}
Valor total das viagens considerando o valor máximo por km, subtraído pelo valor efetivo por km (R$)
{% enddocs %}

{% docs versao_modelo %}
Código de controle de versão [SHA do GitHub]
{% enddocs %}