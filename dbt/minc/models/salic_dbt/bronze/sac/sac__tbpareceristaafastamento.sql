-- Bronze SALIC — sac__tbpareceristaafastamento.
-- Origem: salic_bronze.sac__tbpareceristaafastamento, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 12 colunas: 10 tipadas, 1 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpareceristaafastamento") }} as idpareceristaafastamento,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("idmotivo") }} as idmotivo,
    {{ bronze_timestamp("dtinicioafastamento") }} as dtinicioafastamento,
    {{ bronze_timestamp("dtfimafastamento") }} as dtfimafastamento,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_inteiro("status") }} as status,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_inteiro("idarquivo") }} as idarquivo,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_timestamp("dtautorizacao") }} as dtautorizacao,
    _fatia
from {{ source("bronze_sac", "sac__tbpareceristaafastamento") }}
