-- Bronze iphan — cópia fiel da coleção 103951, "Instituições Parceiras", de
-- bcr.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_bcr_organizacoes. Sem
-- cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "bcr_organizacoes") }}  -- noqa: AM04
