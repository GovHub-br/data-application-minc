-- Bronze iphan — cópia fiel da coleção 1399, "12 - Bibliografia", de
-- inrc.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_inrc_bibliografia. Sem
-- cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_bibliografia") }}  -- noqa: AM04
