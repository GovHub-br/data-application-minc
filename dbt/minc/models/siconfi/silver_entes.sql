-- Silver siconfi — entes (/entes): os entes da federação, com código IBGE, UF,
-- esfera, região e população do ano da consulta.
-- Origem: bronze_entes (um item da API por linha, tudo varchar), com a
-- requisição e o run de bronze_siconfi_paginas.
--
-- Deduplica pela macro siconfi_tentativa_valida (ADR 0014): por request_hash,
-- a tentativa de fetched_at mais recente; dentro dela, uma linha por
-- (_pagina_id, _item). O resto é tipar. Nada é filtrado pelo conteúdo.
--
-- sigla_uf: o SICONFI grava uf = 'BR' nos estados e no DF (a UF a que o ente
-- pertence é a União). A sigla deles sai do código IBGE, que nos estados é o
-- próprio cod_ibge e nos municípios são os dois primeiros dígitos. O teste
-- siconfi_entes_sigla_uf_confere confere o mapa contra a uf dos municípios.
with
    -- As duas CTEs só são lidas dentro da macro, que o lint não expande.
    bronze_itens as (select * from {{ ref("bronze_entes") }}),  -- noqa: ST03

    bronze_paginas as (select * from {{ ref("bronze_siconfi_paginas") }}),  -- noqa: ST03

    itens as (
        select *  -- noqa: AM04
        from {{ siconfi_tentativa_valida("bronze_itens", "bronze_paginas", "entes") }}
    ),

    uf_sigla as (
        select *
        from
            (
                values
                    (11, 'RO'),
                    (12, 'AC'),
                    (13, 'AM'),
                    (14, 'RR'),
                    (15, 'PA'),
                    (16, 'AP'),
                    (17, 'TO'),
                    (21, 'MA'),
                    (22, 'PI'),
                    (23, 'CE'),
                    (24, 'RN'),
                    (25, 'PB'),
                    (26, 'PE'),
                    (27, 'AL'),
                    (28, 'SE'),
                    (29, 'BA'),
                    (31, 'MG'),
                    (32, 'ES'),
                    (33, 'RJ'),
                    (35, 'SP'),
                    (41, 'PR'),
                    (42, 'SC'),
                    (43, 'RS'),
                    (50, 'MS'),
                    (51, 'MT'),
                    (52, 'GO'),
                    (53, 'DF')
            ) as mapa(codigo_uf, sigla_uf)  -- noqa: LT01
    ),

    tipado as (
        select
            {{ bronze_inteiro("cod_ibge", "bigint") }} as cod_ibge,
            {{ bronze_texto("ente") }} as ente,
            {{ bronze_texto("uf") }} as uf,
            {{ bronze_texto("esfera") }} as esfera,
            {{ bronze_texto("regiao") }} as regiao,
            {{ bronze_booleano("capital") }} as capital,
            {{ bronze_texto("cnpj") }} as cnpj,
            {{ bronze_inteiro("exercicio") }} as exercicio,
            {{ bronze_inteiro("populacao", "bigint") }} as populacao,
            _pagina_id,
            {{ bronze_inteiro("_item") }} as _item,
            request_hash,
            run_id,
            fetched_at
        from itens
    )

select
    tipado.cod_ibge,
    tipado.ente,
    tipado.uf,
    uf_sigla.sigla_uf,
    tipado.esfera,
    tipado.regiao,
    tipado.capital,
    tipado.cnpj,
    tipado.exercicio,
    tipado.populacao,
    tipado._pagina_id,
    tipado._item,
    tipado.request_hash,
    tipado.run_id,
    tipado.fetched_at
from tipado
left join
    uf_sigla
    on uf_sigla.codigo_uf = case
        when tipado.cod_ibge between 10 and 99
        then tipado.cod_ibge
        when tipado.cod_ibge between 1000000 and 9999999
        then tipado.cod_ibge / 100000
    end
