-- Bronze SALIC — sac__vwprojetoorgao.
-- Origem: salic_bronze.sac__vwprojetoorgao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 15 colunas: 6 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("iddistribuirparecer") }} as iddistribuirparecer,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("nrprojeto") }} as nrprojeto,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("produto") }} as produto,
    {{ bronze_texto("dtdevolucao") }} as dtdevolucao,
    {{ bronze_texto("descricaoanalise") }} as descricaoanalise,
    {{ bronze_texto("tipoanalise") }} as tipoanalise,
    {{ bronze_texto("obs") }} as obs,
    {{ bronze_texto("area") }} as area,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_timestamp("dtenvio") }} as dtenvio,
    _fatia
from {{ source("bronze_sac", "sac__vwprojetoorgao") }}
