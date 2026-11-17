-- Bronze iphan — cópia fiel da coleção 2052, "04 - Comunidade", de inrc.iphan.gov.br,
-- que a tainacan_dag grava em seeds.iphan_inrc_03_1identificacao. Sem cast, filtro
-- nem join (ADR 0010): tudo continua text, e o formulário da coleção segue em
-- `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_03_1identificacao") }}  -- noqa: AM04
