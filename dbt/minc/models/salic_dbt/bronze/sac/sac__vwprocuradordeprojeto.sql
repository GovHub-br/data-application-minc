-- Bronze SALIC — sac__vwprocuradordeprojeto.
-- Origem: salic_bronze.sac__vwprocuradordeprojeto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 20 colunas: 6 tipadas, 13 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_texto("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_texto("cnpjcpf_proponente") }} as cnpjcpf_proponente,
    {{ bronze_texto("proponente") }} as proponente,
    {{ bronze_inteiro("idprocuracao") }} as idprocuracao,
    {{ bronze_texto("cnpjcpfprocurador") }} as cnpjcpfprocurador,
    {{ bronze_texto("procurador") }} as procurador,
    {{ bronze_inteiro("iddocumento") }} as iddocumento,
    {{ bronze_timestamp("dtprocuracao") }} as dtprocuracao,
    {{ bronze_timestamp("dtvinculacao") }} as dtvinculacao,
    {{ bronze_texto("dtdesvinculacao") }} as dtdesvinculacao,
    {{ bronze_texto("dsjustificativa") }} as dsjustificativa,
    {{ bronze_texto("dsobservacao") }} as dsobservacao,
    {{ bronze_inteiro("idsolicitante") }} as idsolicitante,
    {{ bronze_texto("cpfsolicitante") }} as cpfsolicitante,
    {{ bronze_texto("nomesolicitante") }} as nomesolicitante,
    {{ bronze_texto("siestado") }} as siestado,
    {{ bronze_texto("siprocuracao") }} as siprocuracao,
    _fatia
from {{ source("bronze_sac", "sac__vwprocuradordeprojeto") }}
