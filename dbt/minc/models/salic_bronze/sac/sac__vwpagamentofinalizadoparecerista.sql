-- Bronze SALIC — sac__vwpagamentofinalizadoparecerista.
-- Origem: salic_bronze.sac__vwpagamentofinalizadoparecerista, onde tudo chega como
-- texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 29 colunas: 10 tipadas, 18 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("tppagamento") }} as tppagamento,
    {{ bronze_inteiro("idpagamento") }} as idpagamento,
    {{ bronze_texto("idparecerista") }} as idparecerista,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    {{ bronze_texto("nmparecerista") }} as nmparecerista,
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("nrpronac") }} as nrpronac,
    {{ bronze_texto("nmprojeto") }} as nmprojeto,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_texto("nmarea") }} as nmarea,
    {{ bronze_texto("segmento") }} as segmento,
    {{ bronze_texto("nmsegmento") }} as nmsegmento,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("nmsituacao") }} as nmsituacao,
    {{ bronze_inteiro("idproduto") }} as idproduto,
    {{ bronze_texto("nmproduto") }} as nmproduto,
    {{ bronze_texto("idunidade") }} as idunidade,
    {{ bronze_texto("sgunidade") }} as sgunidade,
    {{ bronze_texto("dtdistribuicao") }} as dtdistribuicao,
    {{ bronze_texto("dthomologacao") }} as dthomologacao,
    {{ bronze_texto("nrempenho") }} as nrempenho,
    {{ bronze_inteiro("nrdespacho") }} as nrdespacho,
    {{ bronze_timestamp("dtgeracaopagamento") }} as dtgeracaopagamento,
    {{ bronze_timestamp("dtefetivacaopagamento") }} as dtefetivacaopagamento,
    {{ bronze_timestamp("dtordembancaria") }} as dtordembancaria,
    {{ bronze_texto("nrordembancaria") }} as nrordembancaria,
    {{ bronze_texto("sipagamento") }} as sipagamento,
    {{ bronze_numerico("vlpagamento") }} as vlpagamento,
    _fatia
from {{ source("bronze_sac", "sac__vwpagamentofinalizadoparecerista") }}
