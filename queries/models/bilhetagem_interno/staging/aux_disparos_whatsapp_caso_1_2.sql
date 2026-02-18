{# {{
    config(
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}} #}
{{
    config(
        materialized="ephemeral",
    )
}}

/*
    Case 1+2: Cidadãos próximos a estações BRT + CadÚnico
    Filtros:
    1) Pessoa Física;
    2) Geolocalizada e moradora da cidade do Rio;
    3) Viva;
    4) Maior de idade;
    5) Com telefone;
    6) Sem Jaé;
    7) Não excluído no CadÚnico;
    8) Até 1 km de distância de uma estação BRT.
*/
with
    /* 1. Pessoas físicas cadastradas na Jaé */
    pessoa_fisica_jae as (
        select distinct nr_documento as cpf, nm_cliente as nome
        from `rj-smtr.cadastro_staging.cliente`
        where in_tipo_pessoa_fisica_juridica = "F"  /* Apenas Pessoas Físicas */
    /* AND cd_tipo_documento = "1" -- CPF - Há CPF que não está com esse tipo, melhor deixar de enviar para alguém que não tenha Jaé do que enviar nesse caso */
    ),
    /* 2. Pessoas Físicas RMI */
    pessoa_fisica_rmi as (
        select *,
        from `rj-crm-registry-dev.patricia__crm_dados_mestres.pessoa_fisica`
        where
            endereco.indicador is true
            and (
                endereco.principal.municipio = "Rio de Janeiro"
                or endereco.principal.municipio is null
            )
            and endereco.principal.logradouro is not null
            and obito.indicador = false
            and menor_idade = false
            and telefone.indicador = true
            and telefone.principal.valor is not null
            and assistencia_social.indicador = true
            and assistencia_social.cadunico.status_cadastral != "Excluido"
            and date_diff(current_date(), nascimento.data, year) >= 18
            and not (
                endereco.principal.latitude is null
                or endereco.principal.longitude is null
            )
    ),
    /* 3. Pessoas Físicas RMI sem Jaé */
    pessoa_fisica_rmi_sem_jae as (
        select r.*
        from pessoa_fisica_rmi as r
        left join pessoa_fisica_jae as j using (cpf)
        where j.nome is null  /* Não está cadastrado na Jaé */
    ),
    /* 4. Último GTFS vigente */
    feed_info as (
        select max(feed_start_date)
        from `rj-smtr.gtfs.feed_info`
        where feed_start_date >= "2025-06-01"
    ),
    /* 5. Estações BRT */
    estacao_brt as (
        select distinct stop_lon, stop_lat
        from `rj-smtr`.`gtfs`.`stops`
        where
            location_type = "1"
            and feed_start_date in (select * from feed_info)
            and (
                stop_code like "TO%"
                or stop_code like "TL%"
                or stop_code like "TC%"
                or stop_code like "TB%"
            )  /* Apenas estações do BRT*/
    ),
    /* 6. Cria objeto geográfico com raio de 1 km de todas as estações BRT */
    estacao_brt_geo as (
        select
            st_union(
                array_agg(st_buffer(st_geogpoint(stop_lon, stop_lat), 1000))
            ) as geo_brt
        from estacao_brt
    ),
    /* 7. Pessoa Física Geolocalizada RMI (sem Jaé) */
    pessoa_fisica_geolocalizada as (
        select
            *,
            st_geogpoint(
                safe_cast(endereco.principal.longitude as float64),
                safe_cast(endereco.principal.latitude as float64)
            ) as geopoint_endereco
        from pessoa_fisica_rmi_sem_jae
    )  /* R.: 1) Pessoa Física; 2) Geolocalizada e moradora da cidade DO Rio; 3) Viva; 4) Maior de idade; 5) Com telefone; 6) Sem Jaé */
select cpf_particao, nome, telefone
from pessoa_fisica_geolocalizada
join estacao_brt_geo on st_intersects(geopoint_endereco, geo_brt)
