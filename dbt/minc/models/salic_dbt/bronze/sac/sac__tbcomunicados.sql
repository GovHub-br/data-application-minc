-- Bronze SALIC — sac__tbcomunicados.
-- Origem: salic_bronze.sac__tbcomunicados, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 8 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idcomunicado") }} as idcomunicado,
    {{ bronze_texto("comunicado") }} as comunicado,
    {{ bronze_inteiro("idsistema") }} as idsistema,
    {{ bronze_texto("stopcao") }} as stopcao,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_timestamp("dtiniciovigencia") }} as dtiniciovigencia,
    {{ bronze_timestamp("dtterminovigencia") }} as dtterminovigencia,
    {{ bronze_inteiro("idedital") }} as idedital,
    {{ bronze_booleano("sifavoritar") }} as sifavoritar,
    {{ bronze_inteiro("idimagem") }} as idimagem,
    _fatia
from {{ source("bronze_sac", "sac__tbcomunicados") }}
