-- Silver SALIC — silver_sac_vwanalisevisualportecnico (ex-sac__vwanalisevisualportecnico).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vwanalisevisualportecnico, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 3 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("tecnico") }} as tecnico,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_timestamp("dtmovimentacao") }} as dtmovimentacao,
    {{ bronze_texto("idorgao") }} as idorgao,
    {{ bronze_texto("conformidadeok") }} as conformidadeok,
    {{ bronze_texto("qtdedias") }} as qtdedias,
    _fatia
from {{ source("salic", "bronze_sac_vwanalisevisualportecnico") }}
