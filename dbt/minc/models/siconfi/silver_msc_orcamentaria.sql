-- Silver siconfi — msc_orcamentaria (/msc_orcamentaria): Matriz de Saldos
-- Contábeis, classes orçamentárias. Uma linha por item da MSC como a API
-- devolve: ente, mês, matriz, conta contábil e as informações complementares
-- (poder/órgão, função, subfunção, natureza, fonte, complemento), com o valor
-- e a natureza do saldo (D/C). No recorte do Eixo 1, dezembro, classe 6, saldo
-- final, de onde sai o empenho em cultura.
-- Origem: bronze_msc_orcamentaria (um item da API por linha, tudo varchar), com
-- a requisição e o run de bronze_siconfi_paginas.
--
-- Deduplica pela macro siconfi_tentativa_valida (ADR 0014): por request_hash,
-- a tentativa de fetched_at mais recente; dentro dela, uma linha por
-- (_pagina_id, _item). O resto é tipar.
--
-- Os códigos (função, subfunção, fonte, complemento, conta, natureza, poder e
-- órgão) ficam como texto, como a API manda: o zero à esquerda é parte do
-- código ('01' é a função Legislativa). Nada é filtrado pelo conteúdo: matriz,
-- mês, classe, natureza do saldo e conta são escolha de quem usa (a Gold).
with
    -- As duas CTEs só são lidas dentro da macro, que o lint não expande.
    bronze_itens as (select * from {{ ref("bronze_msc_orcamentaria") }}),  -- noqa: ST03

    bronze_paginas as (select * from {{ ref("bronze_siconfi_paginas") }}),  -- noqa: ST03

    itens as (
        select *  -- noqa: AM04
        from
            {{
                siconfi_tentativa_valida(
                    "bronze_itens", "bronze_paginas", "msc_orcamentaria"
                )
            }}
    )

select
    {{ bronze_inteiro("cod_ibge", "bigint") }} as cod_ibge,
    {{ bronze_inteiro("exercicio") }} as exercicio,
    {{ bronze_inteiro("mes_referencia") }} as mes_referencia,
    {{ bronze_timestamp("data_referencia") }}::date as data_referencia,
    {{ bronze_texto("tipo_matriz") }} as tipo_matriz,
    {{ bronze_inteiro("classe_conta") }} as classe_conta,
    {{ bronze_texto("tipo_valor") }} as tipo_valor,
    {{ bronze_texto("conta_contabil") }} as conta_contabil,
    {{ bronze_texto("natureza_conta") }} as natureza_conta,
    {{ bronze_texto("poder_orgao") }} as poder_orgao,
    {{ bronze_texto("funcao") }} as funcao,
    {{ bronze_texto("subfuncao") }} as subfuncao,
    {{ bronze_texto("educacao_saude") }} as educacao_saude,
    {{ bronze_texto("natureza_despesa") }} as natureza_despesa,
    {{ bronze_texto("natureza_receita") }} as natureza_receita,
    {{ bronze_inteiro("ano_fonte_recursos") }} as ano_fonte_recursos,
    {{ bronze_texto("fonte_recursos") }} as fonte_recursos,
    {{ bronze_texto("complemento_fonte") }} as complemento_fonte,
    {{ bronze_inteiro("ano_inscricao") }} as ano_inscricao,
    {{ bronze_inteiro("entrada_msc", "bigint") }} as entrada_msc,
    {{ bronze_numerico_cientifico("valor") }} as valor,
    _pagina_id,
    {{ bronze_inteiro("_item") }} as _item,
    request_hash,
    run_id,
    fetched_at
from itens
