-- Bronze SALIC — sac__documentopessoal.
-- Origem: salic_bronze.sac__documentopessoal, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 5 tipadas, 7 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddocumentopessoal") }} as iddocumentopessoal,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("codigo") }} as codigo,
    {{ bronze_inteiro("seq") }} as seq,
    {{ bronze_texto("campo1") }} as campo1,
    {{ bronze_texto("campo2") }} as campo2,
    {{ bronze_texto("campo3") }} as campo3,
    {{ bronze_texto("campo4") }} as campo4,
    {{ bronze_texto("campo5") }} as campo5,
    {{ bronze_booleano("status") }} as status,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("bronze_sac", "sac__documentopessoal") }}
