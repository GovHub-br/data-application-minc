-- Bronze SALIC — sac__vwprojetobeneficiarioprodutocultural.
-- Origem: salic_bronze.sac__vwprojetobeneficiarioprodutocultural, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 7 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_inteiro("tptipicidade") }} as tptipicidade,
    {{ bronze_inteiro("tptipologia") }} as tptipologia,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("beneficiario") }} as beneficiario,
    {{ bronze_inteiro("idtipobeneficiario") }} as idtipobeneficiario,
    {{ bronze_inteiro("qtrecebida") }} as qtrecebida,
    {{ bronze_texto("regiao") }} as regiao,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("municipio") }} as municipio,
    {{ bronze_inteiro("anopublicacao") }} as anopublicacao,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetobeneficiarioprodutocultural") }}
