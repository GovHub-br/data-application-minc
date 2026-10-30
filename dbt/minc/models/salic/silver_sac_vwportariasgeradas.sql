-- Silver SALIC — silver_sac_vwportariasgeradas (ex-sac__vwportariasgeradas).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwportariasgeradas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 1 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("dsreadequacao") }} as dsreadequacao,
    {{ bronze_texto("tipoportaria") }} as tipoportaria,
    _fatia
from {{ source("salic", "bronze_sac_vwportariasgeradas") }}
