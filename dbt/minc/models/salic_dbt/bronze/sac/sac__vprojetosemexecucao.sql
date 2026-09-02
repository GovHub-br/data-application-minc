-- Bronze SALIC — sac__vprojetosemexecucao.
-- Origem: salic_bronze.sac__vprojetosemexecucao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 5 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_inteiro("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_numerico("aprovado") }} as aprovado,
    {{ bronze_numerico("captado") }} as captado,
    {{ bronze_numerico("saldoacaptar") }} as saldoacaptar,
    {{ bronze_inteiro("codigoarea") }} as codigoarea,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_texto("enquadramento") }} as enquadramento,
    _fatia
from {{ source("bronze_sac", "sac__vprojetosemexecucao") }}
