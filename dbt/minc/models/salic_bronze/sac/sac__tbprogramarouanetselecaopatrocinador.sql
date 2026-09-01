-- Bronze SALIC — sac__tbprogramarouanetselecaopatrocinador.
-- Origem: salic_bronze.sac__tbprogramarouanetselecaopatrocinador, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprogramarouanetselecaopatrocinador") }}
    as idprogramarouanetselecaopatrocinador,
    {{ bronze_texto("idprogramarouanet") }} as idprogramarouanet,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idpatrocinador") }} as idpatrocinador,
    {{ bronze_inteiro("stopcao") }} as stopcao,
    {{ bronze_texto("siselecionado") }} as siselecionado,
    _fatia
from {{ source("bronze_sac", "sac__tbprogramarouanetselecaopatrocinador") }}
