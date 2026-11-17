-- Bronze iphan — cópia fiel da coleção 2581, "08 - Grupo", de inrc.iphan.gov.br, que
-- a tainacan_dag grava em seeds.iphan_inrc_06_grupo. Sem cast, filtro nem join (ADR
-- 0010): tudo continua text, e o formulário da coleção segue em `metadata`, como a
-- API o devolveu.
select * from {{ source("iphan", "inrc_06_grupo") }}  -- noqa: AM04
