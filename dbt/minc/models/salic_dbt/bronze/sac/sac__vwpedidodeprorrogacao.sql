-- Bronze SALIC — sac__vwpedidodeprorrogacao.
-- Origem: salic_bronze.sac__vwpedidodeprorrogacao, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 18 colunas: 9 tipadas, 8 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_inteiro("sequencial") }} as sequencial,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("codsituacao") }} as codsituacao,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_timestamp("dtinicioexecucao") }} as dtinicioexecucao,
    {{ bronze_timestamp("dtfimexecucao") }} as dtfimexecucao,
    {{ bronze_texto("mecanismo") }} as mecanismo,
    {{ bronze_inteiro("idprorrogacao") }} as idprorrogacao,
    {{ bronze_timestamp("dtpedido") }} as dtpedido,
    {{ bronze_timestamp("dtinicio") }} as dtinicio,
    {{ bronze_timestamp("dtfinal") }} as dtfinal,
    {{ bronze_texto("justificativa") }} as justificativa,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_texto("atendimento") }} as atendimento,
    _fatia
from {{ source("bronze_sac", "sac__vwpedidodeprorrogacao") }}
