-- Bronze SALIC — sac__tbopinarprojeto.
-- Origem: salic_bronze.sac__tbopinarprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 4 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idopinarprojeto") }} as idopinarprojeto,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idvisao") }} as idvisao,
    {{ bronze_texto("sifaseprojeto") }} as sifaseprojeto,
    {{ bronze_timestamp("dtopiniao") }} as dtopiniao,
    {{ bronze_texto("stquestionamento_1") }} as stquestionamento_1,
    {{ bronze_texto("stquestionamento_2") }} as stquestionamento_2,
    {{ bronze_texto("stquestionamento_3") }} as stquestionamento_3,
    {{ bronze_texto("dscomentario") }} as dscomentario,
    {{ bronze_texto("dsemail") }} as dsemail,
    _fatia
from {{ source("bronze_sac", "sac__tbopinarprojeto") }}
