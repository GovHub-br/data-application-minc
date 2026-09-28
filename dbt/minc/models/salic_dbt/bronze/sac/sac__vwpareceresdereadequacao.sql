-- Bronze SALIC — sac__vwpareceresdereadequacao.
-- Origem: salic_bronze.sac__vwpareceresdereadequacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 6 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("idtiporeadequacao") }} as idtiporeadequacao,
    {{ bronze_texto("dsreadequacao") }} as dsreadequacao,
    {{ bronze_timestamp("dtenviominc") }} as dtenviominc,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_texto("siencaminhamento") }} as siencaminhamento,
    {{ bronze_texto("dsencaminhamento") }} as dsencaminhamento,
    {{ bronze_timestamp("dtparecer") }} as dtparecer,
    {{ bronze_texto("tpmanifestacao") }} as tpmanifestacao,
    {{ bronze_texto("dsmanifestacao") }} as dsmanifestacao,
    {{ bronze_texto("dsparecer") }} as dsparecer,
    _fatia
from {{ source("bronze_sac", "sac__vwpareceresdereadequacao") }}
