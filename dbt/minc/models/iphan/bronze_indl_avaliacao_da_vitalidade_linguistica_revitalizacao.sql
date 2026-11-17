-- Bronze iphan — cópia fiel da coleção 107, "Avaliação da Vitalidade Linguística,
-- Revitalização", de indl.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_indl_avaliacao_da_vitalidade_linguistica_revitalizacao. Sem cast,
-- filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção segue em
-- `metadata`, como a API o devolveu.
select *
from {{ source("iphan", "indl_avaliacao_da_vitalidade_linguistica_revitalizacao") }}  -- noqa: AM04
