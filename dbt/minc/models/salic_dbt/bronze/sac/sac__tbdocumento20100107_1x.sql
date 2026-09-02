-- Bronze SALIC — sac__tbdocumento20100107_1x.
-- Origem: salic_bronze.sac__tbdocumento20100107_1x, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 10 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddocumento") }} as iddocumento,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("stestado") }} as stestado,
    {{ bronze_texto("imdocumento") }} as imdocumento,
    {{ bronze_inteiro("idtipodocumento") }} as idtipodocumento,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_timestamp("dtdocumento") }} as dtdocumento,
    {{ bronze_texto("noarquivo") }} as noarquivo,
    {{ bronze_inteiro("taarquivo") }} as taarquivo,
    {{ bronze_inteiro("idusuariojuntada") }} as idusuariojuntada,
    {{ bronze_timestamp("dtjuntada") }} as dtjuntada,
    {{ bronze_inteiro("idunidadecadastro") }} as idunidadecadastro,
    {{ bronze_texto("codigocorreio") }} as codigocorreio,
    _fatia
from {{ source("bronze_sac", "sac__tbdocumento20100107_1x") }}
