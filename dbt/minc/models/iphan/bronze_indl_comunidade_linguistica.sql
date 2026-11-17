-- Bronze iphan — cópia fiel da coleção 86, "Comunidade Linguística", de
-- indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_comunidade_linguistica. Sem cast, filtro nem join (ADR 0010): tudo
-- continua text, e o formulário da coleção segue em `metadata`, como a API o
-- devolveu.
select * from {{ source("iphan", "indl_comunidade_linguistica") }}  -- noqa: AM04
