-- Bronze SALIC — sac__vmostraempenho.
-- Origem: salic_bronze.sac__vmostraempenho, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 8 tipadas, 4 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("nrempenho") }} as nrempenho,
    {{ bronze_timestamp("dtempenho") }} as dtempenho,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_texto("uggestao") }} as uggestao,
    {{ bronze_inteiro("ugr") }} as ugr,
    {{ bronze_inteiro("ptres") }} as ptres,
    {{ bronze_inteiro("programatrabalho", tipo="bigint") }} as programatrabalho,
    {{ bronze_inteiro("fonterecurso") }} as fonterecurso,
    {{ bronze_inteiro("naturezadespesa") }} as naturezadespesa,
    {{ bronze_texto("usuario") }} as usuario,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    _fatia
from {{ source("bronze_sac", "sac__vmostraempenho") }}
