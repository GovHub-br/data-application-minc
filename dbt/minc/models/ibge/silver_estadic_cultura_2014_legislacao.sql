-- Silver ibge — estadic_cultura_2014_legislacao.
-- Origem: bronze_estadic_cultura_2014_legislacao, cópia fiel de
-- seeds.ibge_estadic_cultura_2014_legislacao, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 22 colunas: 1 tipadas, 21 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("codigouf", "bigint") }} as codigouf,
    {{ bronze_texto("a235") }} as a235,
    {{ bronze_texto("a236") }} as a236,
    {{ bronze_texto("a237") }} as a237,
    {{ bronze_texto("a238") }} as a238,
    {{ bronze_texto("a239") }} as a239,
    {{ bronze_texto("a240") }} as a240,
    {{ bronze_texto("a241") }} as a241,
    {{ bronze_texto("a242") }} as a242,
    {{ bronze_texto("a243") }} as a243,
    {{ bronze_texto("a244") }} as a244,
    {{ bronze_texto("a245") }} as a245,
    {{ bronze_texto("a246") }} as a246,
    {{ bronze_texto("a247") }} as a247,
    {{ bronze_texto("a248") }} as a248,
    {{ bronze_texto("a249") }} as a249,
    {{ bronze_texto("a250") }} as a250,
    {{ bronze_texto("a251") }} as a251,
    {{ bronze_texto("a252") }} as a252,
    {{ bronze_texto("a253") }} as a253,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_texto("dt_ingest") }} as dt_ingest
from {{ ref("bronze_estadic_cultura_2014_legislacao") }}
