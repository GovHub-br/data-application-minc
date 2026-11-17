-- Bronze iphan — cópia fiel da coleção 1954, "03 - Organizações", de
-- inrc.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_inrc_organizacoes. Sem
-- cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_organizacoes") }}  -- noqa: AM04
