-- Bronze iphan — cópia fiel da coleção 48393, "Localidades", de indl.iphan.gov.br,
-- que a tainacan_dag grava em seeds.iphan_indl_modulo_2_identificacao_de_localidades.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text, e o formulário da coleção
-- segue em `metadata`, como a API o devolveu.
select * from {{ source("iphan", "indl_modulo_2_identificacao_de_localidades") }}  -- noqa: AM04
