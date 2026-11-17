-- Bronze iphan — cópia fiel da coleção 2792, "07 - Detentores", de inrc.iphan.gov.br,
-- que a tainacan_dag grava em seeds.iphan_inrc_07_identificacao_detentores. Sem cast,
-- filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção segue em
-- `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_07_identificacao_detentores") }}  -- noqa: AM04
