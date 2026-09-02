-- Bronze SALIC — sac__tbprojetorecebedorrecurso.
-- Origem: salic_bronze.sac__tbprojetorecebedorrecurso, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 8 colunas: 7 tipadas, 0 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojetorecebedorrecurso") }} as idprojetorecebedorrecurso,
    {{ bronze_inteiro("idsolicitacaotransferenciarecursos") }}
    as idsolicitacaotransferenciarecursos,
    {{ bronze_inteiro("idpronactransferidor") }} as idpronactransferidor,
    {{ bronze_inteiro("idpronacrecebedor") }} as idpronacrecebedor,
    {{ bronze_inteiro("tptransferencia") }} as tptransferencia,
    {{ bronze_timestamp("dtrecebimento") }} as dtrecebimento,
    {{ bronze_numerico("vlrecebido") }} as vlrecebido,
    _fatia
from {{ source("bronze_sac", "sac__tbprojetorecebedorrecurso") }}
