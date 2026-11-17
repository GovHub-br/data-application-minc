-- Bronze iphan — cópia fiel da coleção 1933, "02 - Agentes do Patrimônio", de
-- inrc.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_inrc_agentes_do_patrimonio. Sem cast, filtro nem join (ADR 0010): tudo
-- continua text, e o formulário da coleção segue em `metadata`, como a API o
-- devolveu.
select * from {{ source("iphan", "inrc_agentes_do_patrimonio") }}  -- noqa: AM04
