/*
Documentação do Modelo
======================================================

1. Propósito do Modelo
-----------------------
Este modelo tem como objetivo agregar transações de bilhetagem para calcular o número
total de embarques (validações) que ocorreram em cada hexágono geográfico (H3)
para cada dia dentro do período de análise.

2. Fontes de Dados (Entradas)
-----------------------------
- `{{ ref("transacao") }}`: Um modelo dbt que contém os registros individuais de cada
  transação, incluindo o `id_cliente`, a `data` e o ponto geográfico da validação (`geo_point_transacao`).
- `hex`: Uma CTE (ou modelo) que contém a grade de hexágonos H3, mapeando cada `tile_id`
  à sua respectiva forma geométrica (`geometry_hex`).

3. Lógica de Negócio (Passo a Passo)
--------------------------------------
1. **Filtragem por Período:** A consulta começa filtrando a tabela `transacao` para incluir
   apenas os registros que estão dentro do intervalo de datas dinâmico, definido pelas
   variáveis do dbt `date_range_start` e `date_range_end`.
2. **Associação Geográfica (Spatial Join):** Cada transação é associada a um hexágono.
   Isso é feito através da função `ST_CONTAINS`, que verifica se o ponto geográfico
   da transação (`geo_point_transacao`) está contido dentro da geometria de um hexágono.
3. **Agregação:** Após cada transação ter um `tile_id` e uma `data` associados, a
   consulta agrupa todas as linhas por essas duas colunas.
4. **Contagem:** A função `COUNT(*)` é usada para contar o número de transações em
   cada grupo, resultando na coluna `quantidade_embarques`.

4. Estrutura da Saída (Colunas)
-------------------------------
- `data`: A data em que os embarques ocorreram.
- `tile_id`: O identificador único do hexágono H3 onde os embarques ocorreram.
- `quantidade_embarques`: O número total de embarques contados para aquele hexágono
  e dia específicos.

5. Materialização
-----------------
- `table`: O resultado final é salvo como uma tabela física no data warehouse para
  otimizar a performance de consultas subsequentes.

*/
{{
    config(
        materialized="table",
    )
}}

select t.data, h.tile_id, count(*) as quantidade_embarques
from {{ ref("transacao") }} as t
join
    {{ ref("hex") }} as h  -- Assumindo que 'hex' também é um modelo dbt
    on st_contains(h.geometry_hex, t.geo_point_transacao)
where
    t.data between date("{{ var('date_range_start') }}") and date(
        "{{ var('date_range_end') }}"
    )
group by all
