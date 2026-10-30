-- Silver SALIC — silver_sac_tbcredenciaparecerista_2020
-- (ex-sac__tbcredenciaparecerista_2020).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbcredenciaparecerista_2020, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 7 colunas: 4 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcredenciamento") }} as idcredenciamento,
    {{ bronze_inteiro("idparecerista") }} as idparecerista,
    {{ bronze_inteiro("idunidade") }} as idunidade,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_booleano("stativo") }} as stativo,
    _fatia
from {{ source("salic", "bronze_sac_tbcredenciaparecerista_2020") }}
