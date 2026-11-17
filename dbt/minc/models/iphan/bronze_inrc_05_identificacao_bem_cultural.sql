-- Bronze iphan — cópia fiel da coleção 2292, "06 - Bem Cultural", de
-- inrc.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_inrc_05_identificacao_bem_cultural. Sem cast, filtro nem join (ADR
-- 0010): tudo continua text, e o formulário da coleção segue em `metadata`, como a
-- API o devolveu.
select * from {{ source("iphan", "inrc_05_identificacao_bem_cultural") }}  -- noqa: AM04
