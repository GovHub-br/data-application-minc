-- Bronze SALIC — sac__vdadoscomplementaresdecartas.
-- Origem: salic_bronze.sac__vdadoscomplementaresdecartas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 24 colunas: 6 tipadas, 17 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("processo") }} as processo,
    {{ bronze_inteiro("area") }} as area,
    {{ bronze_inteiro("segmento") }} as segmento,
    {{ bronze_texto("cgccpf") }} as cgccpf,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_texto("responsavel") }} as responsavel,
    {{ bronze_texto("endereco") }} as endereco,
    {{ bronze_inteiro("cep") }} as cep,
    {{ bronze_texto("uf") }} as uf,
    {{ bronze_texto("cidade") }} as cidade,
    {{ bronze_texto("nrcarta") }} as nrcarta,
    {{ bronze_timestamp("dtcarta") }} as dtcarta,
    {{ bronze_inteiro("orgao") }} as orgao,
    {{ bronze_texto("dtaprovacao") }} as dtaprovacao,
    {{ bronze_texto("nrportaria") }} as nrportaria,
    {{ bronze_texto("dtportaria") }} as dtportaria,
    {{ bronze_texto("dtpublicacao") }} as dtpublicacao,
    {{ bronze_texto("dtiniciocaptacao") }} as dtiniciocaptacao,
    {{ bronze_texto("dtfimcaptacao") }} as dtfimcaptacao,
    {{ bronze_numerico("saldoacaptar") }} as saldoacaptar,
    _fatia
from {{ source("bronze_sac", "sac__vdadoscomplementaresdecartas") }}
