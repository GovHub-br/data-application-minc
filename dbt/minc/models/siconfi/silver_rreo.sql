-- Silver siconfi — rreo (/rreo): Relatório Resumido da Execução Orçamentária,
-- uma linha por (ente, exercício, período, anexo, conta, coluna) como a API
-- devolve. No recorte do Eixo 1, o Anexo 03 do 6º bimestre, de onde sai a
-- Receita Corrente Líquida.
-- Origem: bronze_rreo (um item da API por linha, tudo varchar), com a
-- requisição e o run de bronze_siconfi_paginas.
--
-- Deduplica pela macro siconfi_tentativa_valida (ADR 0014): por request_hash,
-- a tentativa de fetched_at mais recente; dentro dela, uma linha por
-- (_pagina_id, _item). O resto é tipar. Nada é filtrado pelo conteúdo: qual
-- conta, coluna e período contam é escolha de quem usa (a Gold).
with
    -- As duas CTEs só são lidas dentro da macro, que o lint não expande.
    bronze_itens as (select * from {{ ref("bronze_rreo") }}),  -- noqa: ST03

    bronze_paginas as (select * from {{ ref("bronze_siconfi_paginas") }}),  -- noqa: ST03

    itens as (
        select *  -- noqa: AM04
        from {{ siconfi_tentativa_valida("bronze_itens", "bronze_paginas", "rreo") }}
    )

select
    {{ bronze_inteiro("exercicio") }} as exercicio,
    {{ bronze_inteiro("periodo") }} as periodo,
    {{ bronze_texto("periodicidade") }} as periodicidade,
    {{ bronze_texto("demonstrativo") }} as demonstrativo,
    {{ bronze_texto("anexo") }} as anexo,
    {{ bronze_inteiro("cod_ibge", "bigint") }} as cod_ibge,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("esfera") }} as esfera,
    {{ bronze_texto("instituicao") }} as instituicao,
    {{ bronze_inteiro("populacao", "bigint") }} as populacao,
    {{ bronze_texto("rotulo") }} as rotulo,
    {{ bronze_texto("coluna") }} as coluna,
    {{ bronze_texto("cod_conta") }} as cod_conta,
    {{ bronze_texto("conta") }} as conta,
    {{ bronze_numerico_cientifico("valor") }} as valor,
    _pagina_id,
    {{ bronze_inteiro("_item") }} as _item,
    request_hash,
    run_id,
    fetched_at
from itens
