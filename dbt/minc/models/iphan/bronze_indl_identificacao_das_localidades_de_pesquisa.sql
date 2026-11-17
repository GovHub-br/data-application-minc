-- Bronze iphan — cópia fiel da coleção 26887, "Identificação das localidades de
-- pesquisa", de indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_identificacao_das_localidades_de_pesquisa. Sem cast, filtro nem
-- join (ADR 0010): tudo continua text, e o formulário da coleção segue em `metadata`,
-- como a API o devolveu.
select * from {{ source("iphan", "indl_identificacao_das_localidades_de_pesquisa") }}  -- noqa: AM04
