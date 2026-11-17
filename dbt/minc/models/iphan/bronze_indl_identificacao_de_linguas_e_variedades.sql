-- Bronze iphan — cópia fiel da coleção 20829, "Identificação de línguas e
-- variedades", de indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_identificacao_de_linguas_e_variedades. Sem cast, filtro nem join
-- (ADR 0010): tudo continua text, e o formulário da coleção segue em `metadata`, como
-- a API o devolveu.
select * from {{ source("iphan", "indl_identificacao_de_linguas_e_variedades") }}  -- noqa: AM04
