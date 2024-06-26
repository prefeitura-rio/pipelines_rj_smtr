/*Desaninha os campos presentes em 'content' da tabela stop_details*/
SELECT
  stop_id,
  json_value(content, '$.stop_name') stop_name,
  json_value(content, '$.stop_desc') stop_desc,
  json_value(content, '$.stop_lat') stop_lat,
  json_value(content, '$.stop_lon') stop_lon,
  json_value(content, '$.location_type') location_type,
  json_value(content, '$.Modal') modal,
  json_value(content, '$.Corredor') corredor,
  json_value(content, '$.Tipo de parada') tipo_de_parada ,
  json_value(content, '$.Tipo Sentido') tipo_sentido,
  json_value(content, '$.Propriedade da Parada') propriedade_da_parada,
  json_value(content, '$.Seletivado') seletivado,
  json_value(content, '$.Sinalização') sinalizacao,
  json_value(content, '$.Conservação') conservacao,
  json_value(content, '$.Tipo de abrigo') tipo_de_abrigo,
  json_value(content, '$.Conservação do abrigo') conservacao_do_abrigo,
  json_value(content, '$.Tipo de assento') tipo_do_assento,
  json_value(content, '$.Tipo de baia') tipo_de_baia,
  json_value(content, '$.Tipo de calçada') tipo_de_calcada,
  json_value(content, '$."Possui lixeiras?"') possui_lixeiras,
  json_value(content, '$.Tipo rampa') tipo_rampa,
  json_value(content, '$."Braile?"') braile,
  json_value(content, '$."Piso tátil?"') piso_tatil,
  json_value(content, '$."Elevador?"') elevador,
  json_value(content, '$.Número de vagas') numero_de_vagas,
  json_value(content, '$.Número de cabines') numero_de_cabines,
  json_value(content, '$.AP') ap,
  json_value(content, '$.RA') ra,
  json_value(content, '$.Bairro') bairro,
  json_value(content, '$.Observações') observacoes,
  json_value(content, '$.Endereço') endereco,
  json_value(content, '$.MultiModal') multi_modal,
  json_value(content, '$.id_sequencial') id_sequencial,
  json_value(content, '$.NumeroLinha') numero_linha,
  json_value(content, '$.Vista') vista,
  json_value(content, '$.Horários') horarios,
  json_value(content, '$."Existe o ponto?"') existe_o_ponto,
  json_value(content, '$.parent_station') parent_station,
  json_value(content, '$.nome_ponto') nome_ponto,
  json_value(content, '$.route_type') route_type,
  data_versao

FROM {{ ref("stop_details") }}