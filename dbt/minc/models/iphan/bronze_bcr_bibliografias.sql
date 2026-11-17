-- Bronze iphan — cópia fiel da coleção 110421, "Bibliografia", de bcr.iphan.gov.br,
-- que a tainacan_dag grava em seeds.iphan_bcr_bibliografias. Sem cast, filtro nem
-- join (ADR 0010): tudo continua text, e o formulário da coleção segue em `metadata`,
-- como a API o devolveu.
select * from {{ source("iphan", "bcr_bibliografias") }}  -- noqa: AM04
