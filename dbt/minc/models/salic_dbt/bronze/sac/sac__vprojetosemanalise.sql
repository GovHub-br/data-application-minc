-- Bronze SALIC — sac__vprojetosemanalise.
-- Origem: salic_bronze.sac__vprojetosemanalise, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 0 tipadas, 12 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("orgao") }} as orgao,
    {{ bronze_texto("mecanismo") }} as mecanismo,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("dtsaida") }} as dtsaida,
    {{ bronze_texto("unidadeanalise") }} as unidadeanalise,
    {{ bronze_texto("qtdedias") }} as qtdedias,
    _fatia
from {{ source("bronze_sac", "sac__vprojetosemanalise") }}
