-- Bronze iphan — cópia fiel da coleção 2198, "05 - Território", de inrc.iphan.gov.br,
-- que a tainacan_dag grava em seeds.iphan_inrc_04_identificacao_territorio. Sem cast,
-- filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção segue em
-- `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_04_identificacao_territorio") }}  -- noqa: AM04
