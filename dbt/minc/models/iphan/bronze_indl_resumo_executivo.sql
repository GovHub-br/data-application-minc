-- Bronze iphan — cópia fiel da coleção 184289, "Resumo executivo", de
-- indl.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_indl_resumo_executivo.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "indl_resumo_executivo") }}  -- noqa: AM04
