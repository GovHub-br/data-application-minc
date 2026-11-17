-- Bronze iphan — cópia fiel da coleção 100, "Diagnóstico Sociolinguístico", de
-- indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_diagnostico_sociolinguistico. Sem cast, filtro nem join (ADR
-- 0010): tudo continua text, e o formulário da coleção segue em `metadata`, como a
-- API o devolveu.
select * from {{ source("iphan", "indl_diagnostico_sociolinguistico") }}  -- noqa: AM04
