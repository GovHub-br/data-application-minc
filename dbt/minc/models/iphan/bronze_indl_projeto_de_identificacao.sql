-- Bronze iphan — cópia fiel da coleção 14653, "Projeto de Identificação", de
-- indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_projeto_de_identificacao. Sem cast, filtro nem join (ADR 0010):
-- tudo continua text, e o formulário da coleção segue em `metadata`, como a API o
-- devolveu.
select * from {{ source("iphan", "indl_projeto_de_identificacao") }}  -- noqa: AM04
