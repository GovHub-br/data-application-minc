-- Bronze iphan — cópia fiel da coleção 37719, "0 - Projetos de Identificação", de
-- inrc.iphan.gov.br, que a tainacan_dag grava em seeds.iphan_inrc_2_1_projetos. Sem
-- cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_2_1_projetos") }}  -- noqa: AM04
