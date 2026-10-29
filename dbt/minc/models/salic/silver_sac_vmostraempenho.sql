-- Silver SALIC — silver_sac_vmostraempenho (ex-sac__vmostraempenho).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_vmostraempenho, onde tudo chega como texto da
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
from {{ source("salic", "bronze_sac_vmostraempenho") }}
