-- Silver / núcleo SALIC — dimensão territorial de referência.
--
-- Lê a bronze por `ref`, e nunca a origem diretamente: a bronze é quem aplica
-- os casts, e ler o texto cru aqui desfaria isso sem ninguém notar. O teste de
-- governança verifica isso com busca literal no arquivo, então nem em
-- comentário se escreve a outra forma.
--
-- DUAS FORMAS DE CÓDIGO IBGE. O SALIC identifica o município por um código de
-- 6 posições, sem o dígito verificador; o IBGE e o transferegov usam o de 7. A
-- dimensão expõe as duas, e a correspondência não é calculada: vem de
-- `agentes__populacaomunicipio`, que carrega os dois formatos na mesma linha.
-- Derivar o dígito seria regra a mais para manter, e erraria em silêncio se a
-- regra do IBGE mudasse.
--
-- Os dois chegam da bronze como INTEIRO. Para códigos do IBGE isso é inócuo,
-- porque nenhum começa com zero — o primeiro par identifica a UF, de 11 a 53.
-- Mas o `transferegov` traz o código de 7 posições como texto, então cruzar
-- com ele exige cast explícito de um dos lados.
with municipios as (
    select
        idmunicipioibge as codigo_ibge_6,
        descricao       as nome_municipio,
        idufibge        as id_uf_ibge,
        idmeso          as codigo_mesorregiao,
        idmicro         as codigo_microrregiao
    from {{ ref('agentes__municipios') }}
),

ufs as (
    select
        iduf      as id_uf_ibge,
        sigla     as sigla_uf,
        descricao as nome_uf,
        regiao    as regiao
    from {{ ref('agentes__uf') }}
),

populacao as (
    select
        idmunicipio  as codigo_ibge_6,
        idmunicipio7 as codigo_ibge_7,
        populacao    as populacao
    from {{ ref('agentes__populacaomunicipio') }}
)

select
    m.codigo_ibge_6,
    p.codigo_ibge_7,
    m.nome_municipio,
    m.id_uf_ibge,
    u.sigla_uf,
    u.nome_uf,
    u.regiao,
    m.codigo_mesorregiao,
    m.codigo_microrregiao,
    p.populacao
from municipios as m
-- left join nos dois: município sem UF ou sem população continua na dimensão,
-- com os campos nulos. Descartar aqui esconderia falha de referência que
-- precisa ficar visível — são 5.568 municípios contra 5.563 com população,
-- então a diferença é esperada e tem de aparecer, não sumir.
left join ufs as u
    on m.id_uf_ibge = u.id_uf_ibge
left join populacao as p
    on m.codigo_ibge_6 = p.codigo_ibge_6
