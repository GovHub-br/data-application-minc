-- Bronze iphan — cópia fiel da coleção 3727, "01 - Detalhamento dos Projetos de
-- Identificação", de inrc.iphan.gov.br, que a tainacan_dag grava em
-- seeds.iphan_inrc_2_2_detalhamento_dos_projetos_de_identificacao. Sem cast, filtro
-- nem join (ADR 0010): tudo continua text, e o formulário da coleção segue em
-- `metadata`, como a API o devolveu.
select * from {{ source("iphan", "inrc_2_2_detalhamento_dos_projetos_de_identificacao") }}  -- noqa: AM04
