-- Bronze SALIC — sac__tbplanilhajustificativa.
-- Origem: salic_bronze.sac__tbplanilhajustificativa, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 9 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idplanilhaorcamento") }} as idplanilhaorcamento,
    {{ bronze_inteiro("idplanilhaprojeto") }} as idplanilhaprojeto,
    {{ bronze_timestamp("data") }} as data,
    {{ bronze_numerico("vlsugerido") }} as vlsugerido,
    {{ bronze_booleano("operacaosugerida") }} as operacaosugerida,
    {{ bronze_texto("justificativasugerida") }} as justificativasugerida,
    {{ bronze_numerico("vlaprovado") }} as vlaprovado,
    {{ bronze_booleano("operacaoaprovada") }} as operacaoaprovada,
    {{ bronze_texto("justificativaaprovada") }} as justificativaaprovada,
    {{ bronze_booleano("stestado") }} as stestado,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__tbplanilhajustificativa") }}
