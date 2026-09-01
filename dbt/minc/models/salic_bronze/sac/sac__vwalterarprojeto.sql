-- Bronze SALIC — sac__vwalterarprojeto.
-- Origem: salic_bronze.sac__vwalterarprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 7 tipadas, 6 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_timestamp("dtsituacao") }} as dtsituacao,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("providenciatomada") }} as providenciatomada,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    _fatia
from {{ source("bronze_sac", "sac__vwalterarprojeto") }}
