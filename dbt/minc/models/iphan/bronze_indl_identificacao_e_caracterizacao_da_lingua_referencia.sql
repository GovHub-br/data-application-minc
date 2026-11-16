-- Bronze iphan — cópia fiel da coleção 93, "Identificação e Caracterização da Língua
-- Referência", de indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_identificacao_e_caracterizacao_da_lingua_referencia. Sem cast,
-- filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção segue em
-- `metadata`, como a API o devolveu.
select *
from {{ source("iphan", "indl_identificacao_e_caracterizacao_da_lingua_referencia") }}  -- noqa: AM04
