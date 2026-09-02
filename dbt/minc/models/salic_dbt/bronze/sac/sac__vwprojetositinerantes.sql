-- Bronze SALIC — sac__vwprojetositinerantes.
-- Origem: salic_bronze.sac__vwprojetositinerantes, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 7 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("ufprojeto") }} as ufprojeto,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_texto("tptipicidade") }} as tptipicidade,
    {{ bronze_texto("tptipologia") }} as tptipologia,
    {{ bronze_timestamp("dtiniciofase") }} as dtiniciofase,
    {{ bronze_texto("resumoprojeto") }} as resumoprojeto,
    {{ bronze_inteiro("uf") }} as uf,
    {{ bronze_inteiro("municipio") }} as municipio,
    {{ bronze_texto("idfase") }} as idfase,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetositinerantes") }}
