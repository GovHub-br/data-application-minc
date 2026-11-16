-- Bronze iphan — cópia fiel da coleção 65733, "Bens Culturais Imateriais
-- Registrados", de bcr.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_bcr_bens_culturais. Sem cast, filtro nem join (ADR 0010): tudo continua
-- text, e o formulário da coleção segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "bcr_bens_culturais") }}  -- noqa: AM04
