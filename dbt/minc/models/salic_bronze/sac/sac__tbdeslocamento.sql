-- Bronze SALIC — sac__tbdeslocamento.
-- Origem: salic_bronze.sac__tbdeslocamento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 10 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddeslocamento") }} as iddeslocamento,
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_inteiro("idpaisorigem") }} as idpaisorigem,
    {{ bronze_inteiro("iduforigem") }} as iduforigem,
    {{ bronze_inteiro("idmunicipioorigem") }} as idmunicipioorigem,
    {{ bronze_inteiro("idpaisdestino") }} as idpaisdestino,
    {{ bronze_inteiro("idufdestino") }} as idufdestino,
    {{ bronze_inteiro("idmunicipiodestino") }} as idmunicipiodestino,
    {{ bronze_inteiro("qtde") }} as qtde,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbdeslocamento") }}
