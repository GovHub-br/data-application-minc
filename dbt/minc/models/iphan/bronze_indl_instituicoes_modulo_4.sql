-- Bronze iphan — cópia fiel da coleção 19322, "Escolas", de indl.iphan.gov.br, que a
-- tainacan_dag grava em seeds.iphan_indl_instituicoes_modulo_4. Sem cast, filtro nem
-- join (ADR 0010): tudo continua text, e o formulário da coleção segue em `metadata`,
-- como a API o devolveu.
select * from {{ source("iphan", "indl_instituicoes_modulo_4") }}  -- noqa: AM04
