-- Silver SALIC — silver_sac_tbmensagem (ex-sac__tbmensagem).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_tbmensagem, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idmensagem") }} as idmensagem,
    {{ bronze_texto("nrcpf") }} as nrcpf,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("titulo") }} as titulo,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    {{ bronze_timestamp("dtacesso") }} as dtacesso,
    {{ bronze_timestamp("dtexclusao") }} as dtexclusao,
    {{ bronze_inteiro("idsuccess") }} as idsuccess,
    {{ bronze_numerico("idmulticast") }} as idmulticast,
    _fatia
from {{ source("salic", "bronze_sac_tbmensagem") }}
