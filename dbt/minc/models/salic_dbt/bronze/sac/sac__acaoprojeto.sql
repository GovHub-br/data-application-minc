-- Bronze SALIC — sac__acaoprojeto.
-- Origem: salic_bronze.sac__acaoprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 3 tipadas, 11 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("entidade") }} as entidade,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("tipo") }} as tipo,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_timestamp("dtcriacao") }} as dtcriacao,
    {{ bronze_timestamp("dtaprovacao") }} as dtaprovacao,
    {{ bronze_texto("sumario") }} as sumario,
    {{ bronze_texto("mecanismo") }} as mecanismo,
    {{ bronze_texto("descentidade") }} as descentidade,
    _fatia
from {{ source("bronze_sac", "sac__acaoprojeto") }}
