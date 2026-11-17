-- Bronze iphan — cópia fiel da coleção 5062, "Instituições responsáveis pelos
-- inventários", de indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_instituicoes_responsaveis. Sem cast, filtro nem join (ADR 0010):
-- tudo continua text, e o formulário da coleção segue em `metadata`, como a API o
-- devolveu.
select * from {{ source("iphan", "indl_instituicoes_responsaveis") }}  -- noqa: AM04
