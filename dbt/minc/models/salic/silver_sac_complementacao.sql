-- Silver SALIC — silver_sac_complementacao (ex-sac__complementacao).
-- Lê a Bronze do SALIC, que a ingestão grava em salic.bronze_<banco>_<tabela>.
-- Origem: salic.bronze_sac_complementacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 13 colunas: 9 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("pedido") }} as pedido,
    {{ bronze_timestamp("dtcomplementacao") }} as dtcomplementacao,
    {{ bronze_numerico("solicitadoufir") }} as solicitadoufir,
    {{ bronze_numerico("solicitadoreal") }} as solicitadoreal,
    {{ bronze_numerico("solicitadocusteioufir") }} as solicitadocusteioufir,
    {{ bronze_numerico("solicitadocusteioreal") }} as solicitadocusteioreal,
    {{ bronze_numerico("solicitadocapitalufir") }} as solicitadocapitalufir,
    {{ bronze_numerico("solicitadocapitalreal") }} as solicitadocapitalreal,
    {{ bronze_texto("atendimento") }} as atendimento,
    {{ bronze_inteiro("logon") }} as logon,
    _fatia
from {{ source("salic", "bronze_sac_complementacao") }}
