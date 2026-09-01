-- Bronze SALIC — sac__vrankbanda.
-- Origem: salic_bronze.sac__vrankbanda, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 7 tipadas, 10 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_timestamp("dtprotocolo") }} as dtprotocolo,
    {{ bronze_inteiro("tempodemanda") }} as tempodemanda,
    {{ bronze_texto("pontodemanda") }} as pontodemanda,
    {{ bronze_timestamp("dtfundacao") }} as dtfundacao,
    {{ bronze_inteiro("antiguidade") }} as antiguidade,
    {{ bronze_inteiro("pontoantiguidade") }} as pontoantiguidade,
    {{ bronze_inteiro("finalidade") }} as finalidade,
    {{ bronze_texto("pontofinalidade") }} as pontofinalidade,
    {{ bronze_texto("indicacao") }} as indicacao,
    {{ bronze_texto("pontoindicacao") }} as pontoindicacao,
    {{ bronze_texto("emenda") }} as emenda,
    {{ bronze_texto("pontoemenda") }} as pontoemenda,
    _fatia
from {{ source("bronze_sac", "sac__vrankbanda") }}
