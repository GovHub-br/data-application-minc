-- Bronze iphan — cópia fiel da coleção 106027, "Ações de salvaguarda", de
-- bcr.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_bcr_acoes_de_salvaguarda.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "bcr_acoes_de_salvaguarda") }}  -- noqa: AM04
