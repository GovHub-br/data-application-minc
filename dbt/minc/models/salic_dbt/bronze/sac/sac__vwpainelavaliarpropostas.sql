-- Bronze SALIC — sac__vwpainelavaliarpropostas.
-- Origem: salic_bronze.sac__vwpainelavaliarpropostas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 16 colunas: 10 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_texto("nomeproposta") }} as nomeproposta,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_timestamp("dtmovimentacao") }} as dtmovimentacao,
    {{ bronze_inteiro("diasdesdemovimentacao") }} as diasdesdemovimentacao,
    {{ bronze_inteiro("idmovimentacao") }} as idmovimentacao,
    {{ bronze_texto("codsituacao") }} as codsituacao,
    {{ bronze_timestamp("dtadmissibilidade") }} as dtadmissibilidade,
    {{ bronze_inteiro("diascorridos") }} as diascorridos,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_timestamp("dtavaliacao") }} as dtavaliacao,
    {{ bronze_inteiro("idavaliacaoproposta") }} as idavaliacaoproposta,
    {{ bronze_texto("tecnico") }} as tecnico,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_texto("cnpjcpf") }} as cnpjcpf,
    _fatia
from {{ source("bronze_sac", "sac__vwpainelavaliarpropostas") }}
