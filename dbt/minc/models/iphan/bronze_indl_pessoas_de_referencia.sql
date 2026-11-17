-- Bronze iphan — cópia fiel da coleção 1983, "Pessoas de referência", de
-- indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_pessoas_de_referencia. Sem cast, filtro nem join (ADR 0010): tudo
-- continua text, e o formulário da coleção segue em `metadata`, como a API o
-- devolveu.
select * from {{ source("iphan", "indl_pessoas_de_referencia") }}  -- noqa: AM04
