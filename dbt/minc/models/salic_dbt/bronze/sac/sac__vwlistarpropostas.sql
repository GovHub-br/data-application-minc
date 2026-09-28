-- Bronze SALIC — sac__vwlistarpropostas.
-- Origem: salic_bronze.sac__vwlistarpropostas, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 14 colunas: 8 tipadas, 5 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idprojeto") }} as idprojeto,
    {{ bronze_texto("nomeproposta") }} as nomeproposta,
    {{ bronze_texto("stplanoanual") }} as stplanoanual,
    {{ bronze_inteiro("cnpjcpf", tipo="bigint") }} as cnpjcpf,
    {{ bronze_inteiro("idagente") }} as idagente,
    {{ bronze_inteiro("idusuario") }} as idusuario,
    {{ bronze_texto("tecnico") }} as tecnico,
    {{ bronze_texto("idsecretaria") }} as idsecretaria,
    {{ bronze_timestamp("dtadmissibilidade") }} as dtadmissibilidade,
    {{ bronze_inteiro("dias") }} as dias,
    {{ bronze_inteiro("idavaliacaoproposta") }} as idavaliacaoproposta,
    {{ bronze_inteiro("idmovimentacao") }} as idmovimentacao,
    {{ bronze_texto("sttipodemanda") }} as sttipodemanda,
    _fatia
from {{ source("bronze_sac", "sac__vwlistarpropostas") }}
